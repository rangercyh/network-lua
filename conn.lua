local buffer_queue = require "logic/net/buffer_queue"
local socket = require "socket"

local DEF_MSG_HEADER_LEN = 2
local DEF_MSG_ENDIAN = "big"

local mt = {}

local function resolve(host)
    local addr_tbl, err = socket.dns.getaddrinfo(host)
    if not addr_tbl then
        return false, err.."["..tostring(host).."]"
    end
    return assert(addr_tbl[1])
end

local function connect(addr, port)
    -- just support tcp now
    if not(addr.family == "inet" or addr.family == "inet6") then
        return nil, "just support tcp now"
    end
    local c, e = socket.tcp()
    if not c then return nil, e end
    local fd, e = c:connect(addr.addr, port)
    if not fd then
        c:close()
        return nil, e
    else
        c:settimeout(0) -- non blocking
        local raw = {
            v_send_buf = buffer_queue.create(),
            v_recv_buf = buffer_queue.create(),
            v_fd = c,

            o_host_addr = addr,
            o_port = port,
            v_check_connect = true,
       }
       return setmetatable(raw, {__index = mt})
   end
end

local function connect_host(host, port)
    local addr, err = resolve(host)
    if not addr then
        return false, err
    end
    return connect(addr, port)
end

local function _flush_send(self)
    local send_buf = self.v_send_buf
    local v = send_buf:get_head_data()
    local fd = self.v_fd
    local count = 0

    while v do
        local len = #v
        local n, err, last_sent = fd:send(v)
        if not n then
            if last_pos and last_sent > 0 then
                send_buf:pop(last_sent)
            end
            return false, err
        else
            count = count + n
            send_buf:pop(n)
            if n < len then
                break
            end
        end
        v = send_buf:get_head_data()
    end
    return count
end

local function _flush_recv(self)
    local recv_buf = self.v_recv_buf
    local fd = self.v_fd
    local count = 0

    while true do
    ::CONTINUE::
        local data, err, piece_data = fd:receive("*a")
        if not data then
            if piece_data and #piece_data > 0 then
                recv_buf:push(piece_data)
            end
            if err == "timeout" then
                return true
            end
            return false, err
        else
            local len = #data
            count = count + len
            recv_buf:push(data)
            break
        end
    end

    return count
end

local function _check_connect(self)
    local fd = self.v_fd
    if not fd then
        return false, 'fd is nil'
    end

    if self.v_check_connect then
        local _, wfd, err = socket.select(nil, {fd}, 0)
        if wfd and wfd[fd] then
            self.v_check_connect = false
            return true
        else
            if err == "timeout" then
                return false, "connecting"
            end
            return false, err
        end
    else
        return true
    end
end

function mt:send_msg(data, header_len, endian)
    local send_buf = self.v_send_buf
    header_len = header_len or DEF_MSG_HEADER_LEN
    endian = endian or DEF_MSG_ENDIAN

    send_buf:push_block(data, header_len, endian)
end

function mt:recv_msg(out_msg, header_len, endian)
    local recv_buf = self.v_recv_buf
    header_len = header_len or DEF_MSG_HEADER_LEN
    endian = endian or DEF_MSG_ENDIAN

    return recv_buf:pop_all_block(out_msg, header_len, endian)
end

--[[
update 接口现在会返回三个参数 success, err, status

success: boolean类型 表示当前status是否正常
    true: err返回值为nil
    false: err返回值为string，描述错误信息

err: string类型 表示当前status的错误信息，在success 为false才会有效

status: string类型 当前sconn所在的状态，状态只能是:
    "connect": 连接状态
    "forward": 连接成功状态
    "recv": 接受状态数据状态
    "send": 发送数据状态
    "close": 关闭状态
]]

function mt:update()
    local fd = self.v_fd
    if not fd then
        return false, "fd is nil", "close"
    end

    local success, err = _check_connect(self)
    if not success then
        if err == "connecting" then
            return true, nil, "connect"
        else
            return false, err, "connect"
        end
    end

    success, err = _flush_send(self)
    if not success then
        return false, err, "send"
    end

    success, err = _flush_recv(self)
    if not success then
        if err == "connect_break" then
            return false, "connect break", "connect_break"
        else
            return false, err, "recv"
        end
    end

    return true, nil, "forward"
end

function mt:flush_send()
    local count = false
    repeat
        count = _flush_send(self)
    until not count or count == 0
end

function mt:getsockname()
    return self.v_fd:getsockname()
end

function mt:close()
    if self.v_fd then
        self:flush_send()
        self.v_fd:close()
    end
    self.v_fd = nil
    self.v_check_connect = true
end

return {
    resolve = resolve,
    connect = connect,
    connect_host = connect_host,
}
