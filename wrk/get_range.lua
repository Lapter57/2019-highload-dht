counter1 = 0
counter2 = 1

request = function()
    path = "/v0/entities?start=key" .. counter1 .. "&end=key" .. counter2
    wrk.method = "GET"
    counter1 = counter1 + 1
    counter2 = counter2 + 1
    return wrk.format(nil, path)
end
