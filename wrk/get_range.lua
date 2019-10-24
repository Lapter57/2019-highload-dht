startCounter = 0
endCounter = 1

request = function()
    path = "/v0/entities?start=key" .. startCounter .. "&end=key" .. endCounter
    wrk.method = "GET"
    startCounter = startCounter + 1
    endCounter = endCounter + 1
    if startCounter > 10 then
        startCounter = 0
        endCounter = 1
    end
    return wrk.format(nil, path)
end
