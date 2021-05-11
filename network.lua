local cmsgpack = require "cmsgpack"
local conn = require "conn"
local sconn = require "sconn"

local get_time = os.time

local mt = {}

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

function mt:connect(host, port, stable)
    self.v_request_session = {}
    local conn_module = stable and sconn or conn
    local obj, err = conn_module.connect_host(host, port)
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
    local msg = cmsgpack.unpack(resp)
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
        if handle then
            handle(msg.data)
        elseif self.v_handler then
            self.v_handler(msg.data, dumpcmd)
        end
    else
        error("error dispatch type:"..tostring(msg))
    end
end


function mt:set_defaulthandler(handler)
    self.v_handler = handler
end

function mt:set_timestamp(timestamp)
    self.v_timestamp = timestamp
end

function mt:get_timestamp()
    if self.v_timestamp then
        return self.v_timestamp
    else
        return get_time()
    end
end

function mt:reconnect(cb)
    local conn_module = self.v_conn
    if conn_module.reconnect then
        local ok, err = conn_module:reconnect(function(success)
            print("end reconnect", success)
            if success then -- 重连成功
            end
            cb(success)
        end)
        print("begin reconnect", ok, err)
    end
end

function mt:update()
    local success, err, status = self.v_conn:update()

    if success then
        local out = self.v_out
        local count = self.v_conn:recv_msg(out)
        for i=1,count do
            local resp = out[i]
            local ok, err = xpcall(dispatch, debug.traceback, self, resp)
            if not ok then
                error("error dispatch: ".. err)
            end
        end
    end

    return success, err, status
end

local function request(self, name, t, session_index)
    local req = {
        session = session_index,
        cmd = name,
        data = t,
        timestamp = self:get_timestamp(),
    }
    local ok, msg = xpcall(cmsgpack.pack, debug.traceback, req)
    if not ok then
        error("error cmsgpack data: ".. msg)
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
