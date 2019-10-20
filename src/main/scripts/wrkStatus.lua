request = function()
    path = "/v0/status"
    wrk.method = "GET"
    return wrk.format(nil, path)
end
