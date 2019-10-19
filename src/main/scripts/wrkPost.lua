--
-- Created by IntelliJ IDEA.
-- User: arseny
-- Date: 04.10.2019
-- Time: 17:12
-- To change this template use File | Settings | File Templates.
--

counter = 0

request = function()
    path = "/v0/entity?id=key" .. counter
    wrk.method = "PUT"
    wrk.body = "value" .. counter
    counter = counter + 1
    return wrk.format(nil, path)
end

