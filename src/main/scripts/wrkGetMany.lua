counter = 0

request = function()
    path = "/v0/entities?start=key" .. counter .. "&end=" .. counter + 1000
    wrk.method = "GET"
    counter = counter + 1
    return wrk.format(nil, path)
end
