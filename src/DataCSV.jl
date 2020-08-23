module DataCSV
# Use CSV files to store huge data
# Where colomn :Data stores large data as Strings, while all other columns store keys.

export CSVInfo, dict2Row, row2File, dict2File, file2Rows,  findRows, keyExists, file2Keys, headFirst, iterForward

using DataFrames
using CSV

struct CSVInfo
    keys::Set{Symbol}
    keytypes::Dict{Symbol, Type}
    fileName::String
end

function CSVInfo(sample::Union{Dict, NamedTuple}, fileName::String)
    if (:Data in keys(sample))
        println("Warning: Column named :Data is occupied. Get another name.")
    end
    keytypes = Dict([k => typeof(sample[k]) for k in keys(sample)])
    CSVInfo(
        Set(keys(sample)),
        merge(keytypes, Dict(:Data=> String)),
        fileName
    )
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

function useRow(r; nokeys = false, dmapper= identity)
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


function headFirst(keys::Array, ranges::Dict, f::Function; n::Int = 1, pace::Int = -1)::Dict
    # Find the unique point (p) in a sequence where f (p)  turns from true to false,
    # where the sequence is ordered by iterating and combining the items from  each range
    # and returns the ranges each of which contains the uniterated elements in the local loop.
    # The range must be iterated with order defined by keys, in which the last is looped first.
    if (n > length(keys))
        return Dict()
    end
    k = keys[n]
    rg = ranges[k]
    if (length(rg) <= 1)
        return headFirst( keys, ranges, f; n = n + 1 )
    end
    paceGood = if (pace >= length(rg) || pace <= 0) div(length(rg), 2) else pace end
    
    r1 = rg[1]
    r2 = rg[1 + paceGood]
    param1 = Dict([r => ranges[r][1] for r in keys])
    param2 = merge(param1, Dict(k => r2))
    if (f(param2))
        ranges[k] = ranges[k][1 + paceGood : end]
        return headFirst( keys, ranges, f; n = n)
    elseif paceGood > 1
        headFirst( keys, ranges, f; pace = div(paceGood, 2), n = n)
    else
        # between rg and rg.tail
        out = headFirst( keys, copy(ranges), f; n = n + 1)
        if (isempty(out))
            ranges1 = merge(ranges, Dict(k => rg[2 : end]))
            return ranges1
        else
           return out
        end
    end
end

function iterFromInit(f::Function, keys::AbstractArray, ranges::Dict; init = ranges) 
    function worker(ks::AbstractArray, paras::Dict  = Dict(); useInit::Bool = true)
        if (isempty(ks))
            f(paras)
            return
        end
        k = ks[1]
        if useInit
            rg = init[k]
            for r in rg[1 : 1]
                paras1 = merge(paras, Dict(k => r))
                worker(ks[2 : end], paras1; useInit = true)
            end
            for r in rg[2 : end]
                paras1 = merge(paras, Dict(k => r))
                worker(ks[2 : end], paras1; useInit = false)
            end
        else
            rg = ranges[k]
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

function iterForward(f::Function, iterKeys::AbstractArray, iterRanges::Dict, info::CSVInfo; keyForData = (p, d) -> p)
    # Do new iteration or resume the interrupted csv file.
    function runAndSave(paras)
        data::Dict = f( paras )
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


end # module DataCSV
