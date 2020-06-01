local msgpack = require "logic/net/msgpack"
local conn = require "logic/net/conn"

local get_time = os.time

local mt = {}

local DEFAULT_HEARTBEAT_INTERVAL = 30  -- 默认心跳间隔(秒)

local function new()
    local raw = {
        v_session_index = 1,
        v_request_session = {},
        v_response_handle = {},
        v_out = {},
        v_conn = false,
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

function mt:connect(host, port)
    self.v_request_session = {}
    local obj, err = conn.connect_host(host, port)
    if not obj then
        return false, err
    else
        self.v_conn = obj
        return true
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
    local session_item = {
        name = name,
        handle = false,
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
        assert(cb)
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

return new
