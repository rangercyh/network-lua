local msgpack = require "logic/net/msgpack"
local conn = require "logic/net/conn"
local sconn = require "logic/net/sconn"
local co_funcs = reuqire "logic/net/co_funcs"

local get_time = os.time

local mt = {}

local TIMEOUT_LIMIT = 60  --协议整个超时时间为 TIMEOUT_LIMIT 秒, 如果有断线重连，将会进行断线重连，没有的话直接报错了
local INTERVAL_RECONNECT = 10  -- 断线重连超时时间
local DEFAULT_HEARTBEAT_INTERVAL = 30  -- 默认心跳间隔(秒)
local CONN_TOLERATE_TICK = 30 * 3  -- connect连接容忍时间

local function errorf(...)
    error(string.format(...))
end

local function resume_aux(co, ok, err, ...)
    if not ok then
        error(debug.traceback(co, err))
    end
    return err, ...
end

function co_resume(co, ...)
    return resume_aux(co, coroutine.resume(co, ...))
end

local function new()
    local raw = {
        v_session_index = 1,
        v_request_session = {},
        v_response_handle = {},
        v_out = {},
        v_conn = false,

        v_last_reconnect = 0,
        v_reconnect_count = 0,
    }

    return setmetatable(raw, {__index = mt})
end

------------------------------------------------------------
-- 设置心跳消息
-- 若 msg 为 nil 则不发送心跳消息
-- interval 为心跳间隔秒数, 默认为 DEFAULT_HEARTBEAT_INTERVAL
------------------------------------------------------------
function mt:set_heartbeat(msg, interval)
    self.v_heartbeat_msg = msg
    self.v_heartbeat_interval = interval or DEFAULT_HEARTBEAT_INTERVAL
end

function mt:connect(host, port, stable_connect)
    self.v_request_session = {}
    local conn_module = stable_connect and sconn or conn
    local obj, err = conn_module.connect_host(host, port)
    if not obj then
        return false, err
    else
        self.v_conn = obj
        self.o_status = "connecting"
        self.v_cur_thread = coroutine.running()
        self.v_cur_tolerate = 0
        return coroutine.yield()
    end
end

function mt:check_connected()
    return self.v_conn
end

local function dispatch(self, resp)
    local msg = msgpack.unpack(resp)
    local session = msg.session
    local dumpcmd = msg.dumpcmd
    if session and session > 0 then
        local session_item = self.v_request_session[session]
        local handle = session_item.handle
        local tt  = type(handle)
        if tt == "function" then
            handle(msg.data)
        elseif tt == "thread" then
            local success, err = coroutine.resume(handle, msg.data)
            if not success then
                error(err)
            end
        else
            error("error handle type:"..tt.." from msg:"..tostring(session_item.name))
        end
        self.v_request_session[session] = nil
    elseif dumpcmd then
        local handle = self.v_response_handle[dumpcmd]
        if not(handle) then
            errorf("no request handler for '%s'", tostring(dumpcmd))
        end
        handle(msg.data)
    else
        error("error dispatch type:"..tostring(msg))
    end
end

local function check_heartbeat(self)
    local msg = self.v_heartbeat_msg
    local interval = self.v_heartbeat_interval
    if not msg then
        return
    end

    if self.v_wait_heartbeat then
        return
    end

    local cur_time = get_time()
    self.v_last_heartbeat_time = self.v_last_heartbeat_time or cur_time
    if cur_time - self.v_last_heartbeat_time >= interval then
        self.v_wait_heartbeat = true
        self:call(msg, nil, function()
            self.v_wait_heartbeat = nil
            self.v_last_heartbeat_time = cur_time
        end)
    end
end

local function can_reconnect(self)
    local conn = self.v_conn
    return not not (conn and conn.reconnect)
end

-- 捕获网络超时错误
local function handle_timeout_limit(self, err)
    -- 如果不能够断线重连将会直接提示
    if not can_reconnect(self) then
        errorf("request timeout limit : %s", err)
    end
end

local function reset_session_time(self)
    local cur_time = get_time()
    for _, v in pairs(self.v_request_session) do
        v.time = cur_time
    end
end

local function on_reconnect_done(self, success)
    -- 重连成功
    if success then
        local cur_time = get_time()
        print('reconnect successs. '..tostring(cur_time))

        -- 刷新心跳时间
        self.v_last_heartbeat_time = cur_time

        -- 刷新协议超时时间
        reset_session_time(self)

        -- 重置最近重连时间
        self.v_last_reconnect = 0
    end
end

-- 直接进行断线重连
local function do_reconnect(self, reason)
    -- 如果当前网络已经错误， 将不会再做短线重连
    if self.v_is_error then
        return
    end

    local conn = self.v_conn
    local cur_time = get_time()
    self.v_last_reconnect = cur_time
    self.v_reconnect_count = self.v_reconnect_count + 1
    local ok, err = conn:reconnect(function(success)
        return on_reconnect_done(self, success)
    end)
    print("begin reconnect", ok, err)
end

local function check_reconnect(self, duration)
    local conn = self.v_conn
    if conn.reconnect then
        return
    end

    local cur_time = get_time()
    local last_reconnect_time = self.v_last_reconnect

    -- 第一次断线重连
    if last_reconnect_time == 0 then
        if duration >= INTERVAL_RECONNECT then
            do_reconnect(self, "interval_timeout")
        end
    else
        if cur_time - last_reconnect_time >= INTERVAL_RECONNECT then
            do_reconnect(self, "interval_timeout")
        end
    end
end

local function check_timeout_session(self)
    local cur_time = get_time()
    for _, v in pairs(self.v_request_session) do
        local delta_time = cur_time - v._time
        if delta_time >= 1 then
            v._time = cur_time
            local duration = cur_time - v.time
            if v.call_duration then
                v.call_duration = v.call_duration + delta_time
            end
            if not self.v_disable_timeout then
                if duration >= 5 then
                    print(string.format("[timeout] name: %s session: %d time:%d duration:%d call_duration:%s",
                        v.name, v.session, v.time, duration, v.call_duration))
                end

                -- 协议超时异常
                if TIMEOUT_LIMIT and v.call_duration and v.call_duration >= TIMEOUT_LIMIT then
                    handle_timeout_limit(self, "[timeout]:"..tostring(v.name))
                    v.call_duration = nil
                    return
                end
            end

            -- 是否断线重连
            check_reconnect(self, duration)
        end
    end
end

local last_success, last_err, last_status
local function check_network(self, success, err, status)
    if last_success ~= success or last_err ~= err or last_status ~= status then
        print("[check_network]:", success, err, status)
        last_success = success
        last_err = err
        last_status = status
    end

    -- 连接成功
    if success then
        return true
    end

    -- 当前处于正在连接状态
    if err == "connecting" then
        return true
    end

    -- 连接失败
    if status == "connect" then
        print("network connect failed")
        return false
    end

    -- 服务器主动关闭连接
    if err == "connect_break" then
        print("network connect break")
        return false
    end

    -- 无法重连
    if status == 'reconnect' then
        if err == 'reconnect_error' or
           err == 'reconnect_match_error' or
           err == 'reconnect_cache_error' then
            print('network can not reconnect')
            return false
        end
    end

    -- 普通错误
    print(err.." status:"..tostring(status))
    return false
end

function mt:update()
    local success, err, status = self.v_conn:update()

    if success then
        local out = self.v_out
        local conn = self.v_conn
        local count = conn:recv_msg(out)
        for i=1,count do
            local resp = out[i]
            local ok, err = xpcall(dispatch, debug.traceback, self, resp)
            if not ok then
                error("error dispatch: ".. err)
            end
        end
        check_heartbeat(self)
    end

    check_timeout_session(self)

    local thread = self.v_cur_thread
    if self.o_status == 'connecting' and not(success) and thread then
        if self.v_cur_tolerate >= CONN_TOLERATE_TICK then
            if coroutine.status(thread) == 'suspended' then
                self.v_cur_thread = nil
                self:close()
                return co_resume(thread, success, err)
            end
        else
            self.v_cur_tolerate = self.v_cur_tolerate + 1
            return
        end
    end
    if thread then
        self.v_cur_thread = nil
        return co_resume(thread, true)
    end

    self.o_status = err or "forward"

    -- 检查网络状态
    self.v_is_error = not(check_network(self, success, err, status))

    return success, err, status
end

local function request(self, name, t, session_index)
    local req = {
        session = session_index,
        cmd = name,
        data = t,
        timestamp = tonumber(Util.GetTimeStamp()),
    }
    local ok, msg = xpcall(msgpack.pack, debug.traceback, req)
    if not ok then
        error("error msgpack data: ".. msg)
    end
    return self.v_conn:send_msg(msg)
end

function mt:call(name, t, cb)
    local session_index = self.v_session_index
    self.v_session_index = session_index + 1

    assert(self.v_request_session[session_index]==nil, session_index)
    local cur_time = get_time()
    local session_item = {
        session = session_index,
        name = name,
        handle = false,
        time = cur_time,
        _time = cur_time,
        call_duration = 0,
    }
    self.v_request_session[session_index] = session_item

    if cb then
        session_item.handle = cb
        request(self, name, t, session_index)
    elseif coroutine.isyieldable() then
        session_item.handle = coroutine.running()
        request(self, name, t, session_index)
        return coroutine.yield()
    else
        errorf("network call: %s at invalid env", name)
    end
end

function mt:invoke(name, t)
    return request(self, name, t)
end

function mt:register(name, cb)
    assert(cb)
    assert(self.v_response_handle[name] == nil)
    self.v_response_handle[name] = cb
end

function mt:close()
    if self.v_conn then
        self.v_conn:close()
    end
    self.v_conn = false
end

-- 忽略超时协议,不产生警告，继续超时
function mt:disable_timeout(b)
    self.v_disable_timeout = b
end

return new
