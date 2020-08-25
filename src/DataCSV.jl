module DataCSV
# Use CSV files to store huge data
# Where colomn :Data stores large data as Strings, while all other columns store keys.

export CSVInfo, dict2Row, row2File, dict2File, file2Rows,  findRows, keyExists, file2Keys, headFirst, iterForward, iterFromLast, SymbolRange

using DataFrames
using CSV

struct CSVInfo
    keys::Array{Symbol}
    keytypes::Dict{Symbol, Type}
    fileName::String
end

SymbolRange = Dict{Symbol, Union{Function, <: AbstractArray}}

function CSVInfo(sample::Union{Dict, NamedTuple}, fileName::String; skeys = collect(keys(sample)))
    if (:Data in skeys)
        println("Warning: Column named :Data is occupied. Get another name.")
    end
    keytypes = Dict([k => typeof(sample[k]) for k in skeys])
    CSVInfo(
        skeys,
        merge(keytypes, Dict(:Data=> String)),
        fileName
    )
end

function CSVInfo(iKeys::AbstractArray{Symbol}, iRanges::SymbolRange, fileName::String)::CSVInfo
    innerType(x::AbstractArray{T}) where T = T
    tRanges = rangesFirst(iKeys, iRanges)
    keyTypes = Dict{Symbol, Type}(k => innerType(v) for (k, v) in tRanges)
    CSVInfo(
        iKeys,
        merge(keyTypes, Dict(:Data => String)),
        fileName)
end

function dict2Row(d::Union{Dict, NamedTuple}, data::Any, info::CSVInfo)
    df=DataFrame()
    for k in info.keys
        df[!, k] = [d[k]]
    end
    df[!, :Data]=[data]
    df
end

function row2File(t::DataFrame, info::CSVInfo)
    if (! isfile(info.fileName))
        CSV.write(info.fileName, t) 
    else
        CSV.write(info.fileName, t; append=true)
    end
end

function dict2File(d::Union{Dict, NamedTuple}, data::Any, info::CSVInfo)
    row2File(dict2Row(d, data, info), info)
end

function file2Rows(info::CSVInfo)
    rawTable=CSV.Rows(info.fileName; types = info.keytypes)
    rawTable
end

function file2Keys(info::CSVInfo; lazyList = false, select = collect( info.keys))
    if (! isfile(info.fileName))
        return [ ]
    end
    keyColumns = CSV.Rows(info.fileName; select = select, types = info.keytypes) 
    if (lazyList) keyColumns else
        [ Dict([ k => r[k] for k in keys(r)]) for r in keyColumns]
    end
end


function goodRow(key::Union{Dict, NamedTuple}, row)
        for k in keys(key)
            if (key[k]  !=  row[k])
                return false
            end
        end
        return true
end

function useRow(r; nokeys = true, dmapper= identity)
    data=r[:Data]
    try
        data = eval(Meta.parse(r[:Data]))
    catch
        println("data is plain String! ->  $data")
    end
    if (nokeys)
        return dmapper( data )
    else
        keyDict = Dict([k => r[k] for k in keys(r) if k != :Data])
        merge(keyDict, Dict(:Data => data))
    end
end

function findRows( key::Union{Dict, NamedTuple}, rows; iter = false, nokeys = true, dmapper = identity )
    f( r ) = goodRow( key, r )
    findRows( f, rows; iter = iter, nokeys = nokeys, dmapper = dmapper )
end

function findRows(f::Function, rows; iter = false, nokeys = true, dmapper = identity )
    gen = (useRow(r, nokeys = nokeys, dmapper = dmapper ) for r in rows if f(r))
    if (iter)
        Iterators.Stateful( gen )
    else 
        collect(gen)
    end
end

function keyExists(keys::Union{Dict, NamedTuple}, rows)
    for r in rows
        if  (goodRow(keys, r))
            return true
        end
    end
    return false
end

trueRange(rg::AbstractArray, keyMap::Dict) = rg
trueRange(rg::Function, keyMap::Dict) = begin
    rg(keyMap)
end

function rangesFirst(keys::AbstractArray{Symbol}, ranges::SymbolRange; n::Int = 1, keyMap = Dict())
    if (n > length(keys))
        return ranges
    end
    k = keys[n]
    rg  = trueRange(ranges[k], keyMap)
    if (isempty(rg))
         Dict( )
    else
        keyMap1 = merge(keyMap, Dict(k => rg[1]))
        ranges1::Dict = merge(ranges, SymbolRange( k => rg ))
        rangesFirst(keys, ranges1; n = n + 1, keyMap = keyMap1)
    end
end

function headFirst(keys::AbstractArray, ranges::SymbolRange, f::Function; n::Int = 1, pace::Int = -1)::Dict
    # Find the unique point (p) in a sequence where f (p)  turns from true to false,
    # where the sequence is ordered by iterating and combining the items from  each range
    # and returns the ranges each of which contains the uniterated elements in the local loop.
    # The range must be iterated with order defined by keys, in which the last is looped first.
    if (n > length(keys))
        return Dict()
    end
    ranges2 = rangesFirst(keys, ranges)
    if (isempty(ranges2))
        return Dict()
    end
    
    k = keys[n]
    rg = ranges2[k]
    ranges1 = merge(ranges, SymbolRange(k => rg))
    if (length(rg) <= 1)
        return headFirst( keys, ranges1, f; n = n + 1)
    end
    paceGood = if (pace >= length(rg) || pace <= 0) div(length(rg), 2) else pace end    
    
    r1 = rg[1]
    r2 = rg[1 + paceGood]
    param1 = Dict([r => ranges2[r][1] for r in keys])
    param2 = merge(param1, Dict(k => r2))
    if (f(param2))
        ranges1[k] = rg[1 + paceGood : end]
        return headFirst( keys, ranges1, f; n = n )
    elseif paceGood > 1
        #between rg[1] and rg[pace + 1]
        headFirst( keys, ranges1, f; pace = div(paceGood, 2), n = n  )
    else n < length(keys)
        # between rg[1] and rg.[2]
        out = headFirst( keys, ranges1, f; n = n + 1)
        if (isempty(out) )
            if (n < length(keys))
                println("Sigular Situation $k: ranges1=$ranges1")
                ranges1[k] = rg[2 : end]
                return rangesFirst(keys, ranges1)
            else
                return ranges1
            end
        else
           return out
        end
    end
end

function iterFromInit(f::Function, keys::AbstractArray, ranges::SymbolRange; init = ranges) 
    function worker(ks::AbstractArray, paras::Dict  = Dict(); useInit::Bool = true)
        if (isempty(ks))
            f(paras)
            return
        end
        k = ks[1]
        if useInit
            rg = trueRange(init[k], paras)
            for r in rg[1 : 1]
                paras1 = merge(paras, Dict(k => r))
                worker(ks[2 : end], paras1; useInit = true)
            end
            for r in rg[2 : end]
                paras1 = merge(paras, Dict(k => r))
                worker(ks[2 : end], paras1; useInit = false)
            end
        else
            rg = trueRange(ranges[k], paras)
            for r in rg
                paras1 = merge(paras, Dict(k => r))
                worker(ks[2 : end], paras1; useInit = false)
            end
        end
    end
    worker(keys, Dict(); useInit = true)
end


function getRapidChecker(info::CSVInfo)::Function
    if (! isfile(info.fileName))
        return (d) -> false
    end
    oks = file2Keys(info; lazyList = true) 
    function f(k)
        keyExists(k, oks)
    end
    f
end

function iterForward(f::Function, iterRanges::SymbolRange, info::CSVInfo; keyForData = (p, d) -> p, iterKeys::AbstractArray = info.keys )
    # Do new iteration or resume the interrupted csv file.
    function runAndSave(paras)
        data = f( paras )
        dict2File(keyForData(paras, data), data, info)
    end
    checker = getRapidChecker(info)
    init = headFirst(iterKeys, iterRanges, checker)
    iterFromInit(runAndSave, iterKeys, iterRanges; init = init) 
end

function getKeyChecker(info)
    keysInFile = [] 
    if ( isfile(info.fileName))
        println("Found a data file!")
        keysInFile = file2Keys(info)
        println("Finished reading the data file!, $(length(keysInFile)) Items")
    end
    k -> keyExists(k, keysInFile)
end

function readLastLine(info::CSVInfo)
    iName = info.fileName
    oName = ".temp_history.csv"
    run( pipeline(`head -n 1 $iName`, oName) )
    open( io -> run( pipeline(`tail -n 1 $iName`, stdout = io)), oName, "a" )
    lastRow = collect(CSV.Rows(oName; types = info.keytypes ))[end]
    res = Dict([ k => lastRow[k] for k in keys(lastRow) ])
    rm(oName)
    res
end

function getLastKey(iKeys, iRanges, oldOut; n = 1, init = Dict())::SymbolRange
    # Get the final iteration list where the last csv file dies out
    k = iKeys[n]
    target = oldOut[k]
    rg = trueRange( iRanges[k], oldOut )

    dealTarget(l, t) =
        if length( l ) <= 1
            l
        elseif l[ 1 ] == t
            l
        else
            dealTarget( l[ 2 : end ], t )
        end
    
    rg1 = dealTarget(rg, target)
    init[k] = rg1
    if n < length(iKeys)
        getLastKey(iKeys, iRanges, oldOut; n = n + 1, init = init)
    else
        init
    end
end

function getNextKey(iKeys::AbstractArray, iRanges::SymbolRange, tRanges::SymbolRange; n = 1)::Tuple{SymbolRange, Bool} 
    #if ( status )
    #    return (iRanges, true)
    #elseif n > length( iKeys )
    if n > length( iKeys )
        return (iRanges, false)
    else
        k = iKeys[ n ] 
        rg = tRanges[ k ]
        iRanges1 = merge( iRanges, SymbolRange( k => rg ) )
        res, st1= getNextKey( iKeys, iRanges1, tRanges; n = n + 1 )
        if ( st1 )
            return (res, true)
        else
            if length( rg ) > 1
                iRanges[ k ] = rg[ 2 : end ]
                return ( rangesFirst( iKeys, iRanges ), true )
            else
                return ( iRanges, false )
            end
        end
    end
end

function iterFromLast(f::Function,  iRanges::SymbolRange, info::CSVInfo; init = iRanges, keyForData = (p, d) -> p, iKeys::AbstractArray = info.keys,)
    # If iteration has history, resume from the csv file in storage; otherwise do normal iteration.
    init =
        if (isfile( info.fileName ))
            rLast = readLastLine( info )
            println( "Found ancient iteration: $rLast" )
            kLast = getLastKey( iKeys, iRanges, rLast )
            nextIter, st = getNextKey( iKeys, iRanges, kLast )
            nextIter
        else
            init
        end
    function runAndSave(paras)
        data = f( paras )
        dict2File(keyForData(paras, data), data, info)
    end
    iterFromInit(runAndSave, iKeys, iRanges)
end


end # module DataCSV
