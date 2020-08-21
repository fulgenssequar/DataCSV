module DataCSV
# Use CSV files to store huge data
# Where colomn :Data stores large data as Strings, while all other columns store keys.

export CSVInfo, dict2Row, row2File, dict2File, file2Rows,  findRows, keyExists, file2Keys

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

function file2Keys(info)
    if (! isfile(info.fileName))
        return [ ]
    end
    keyColumns = CSV.Rows(info.fileName; select=info.keys, types = info.keytypes) 
    [ Dict([ k => r[k] for k in keys(r)]) for r in keyColumns]
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

function findRows( key::Dict, rows; iter = false, nokeys = true, dmapper = identity )
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
