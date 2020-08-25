# DataCSV: Store and Restore Data Using CSV.jl

## Introduction

### What for:
Gigantic amount of data items can be stored and extracted using Dictionary-like CSV files without exerting the memory.

### Features:
- New data is appended to the csv file in disk without risk of losing in case of shutdown.
- A column named "Data" is preserved to restore the data which is converted into String.
- All other columns are indices for query.
- New data and indices of type Dict can be directly converted into a csv item.

##  Usage

Exported Functions:

- CSVInfo(::Dict, String)::CSVInfo  Returns the object containing the information of the current csv file. 

```julia
sample = ( x = 5, y = 3 )
info = CSVInfo( sample, "./data.csv" )
```
- dict2File(::Dict, ::Any, ::CSVInfo)::Unit Writes item to the csv file.

```julia
for  x in 1:20
    for y in 1:10
        matrix = rand( x % 3 + 1, y % 4 + 1 )
        avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
        dict2File( ( x = x, y = y ), Dict( :matrix  => matrix, :avg => avg ), info )
        print( " Item x = $x, y = $y  and its data is safely stored in CSV    \r" )
    end
end
```

- file2Row( ::CSVInfo )::CSV.Rows2 Find the row iterator of the csv file.

```julia
rows = file2Rows( info ) # rows is a row iterator 
```
- findRows( filter::Function, ::CSV.Rows2; iter = false , dmapper = identity, nokeys = true)
Get the row iterator from rows where filter returns true:

```julia

# Get all matrices of size 2 x 3

# Return a iterator, not a Array -> iter
# Discard all key columns other than the :Data column -> nokeys
# Only return the [:matrix] from the Dict. -> dmapper

mx23 = findRows( r -> r.x % 3 == 1 && r.y % 4 == 2, rows; iter = true, nokeys = true, dmapper = d -> d[:matrix] )
# mx23 can be folded, reduced, aggregated, accumulated, etc.

```
## Intelligent Iteration Using Exported Functions: iterFromLast

In case of computer shutdown, it is possible that the csv file contains only partial lines of the whole desired data.

If the computation restores from the beginning, the old csv file **is appended** with new data and consequently contains **reduplicated** items.

However, checking if a certain key is contained by the old file before computation would be time consuming, while totally discarding the old data seems uneconomical.

So you would want to use the function:
``` julia
# Slower:
function iterForward(f::Function, iterRanges::Dict, info::CSVInfo; keyForData = (p, d) -> p, iterKeys::AbstractArray = info.keys)::Unit
# OR: Faster 
function iterFromLast(f::Function, iterRanges::Dict, info::CSVInfo; keyForData = (p, d) -> p, iterKeys::AbstractArray = info.keys)::Unit
```
which rapidly find the position where the last iteration was interrupted and resume the computation, in addition of doing the normal iteration.

- Function f takes a Dict as input and returns the result data

- iterKeys defines the order of iteration, where the last is looped first ( same as the nested for loop )

- * iterKeys should contain only (not necessarily all) keys indicated in the CSVInfo object. If not all, keyForData(paras, data) should be defined properly.

- iterRanges is a Dict defining the ranges each key loops in.

- Any new data computed by f will be appended to the csv file indicated by info.

```julia
iKeys = [:x, :y]

function getData(paras)
    x = paras[ :x ]
    y = paras[ :y ]
    matrix = rand( x % 3 + 1, y % 4 + 1 )
    avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
    data = SymbolRange(:matrix => matrix, :avg => avg) # SymbolRange isa Dict
    println("Smart Iteration at: x = $x, y = $y")
    data
end

# First iteration:

iRanges1 = SymbolRange(:x => 1:10, :y => 1:10)
iterFromLast(getData, iRanges1, info)

# shutdown and resume 

iRanges2 = SymbolRange(:x => 1:23, :y => 1:10)
iterFromLast(getData, iRanges2, info)

rows = collect(file2Rows(info))

if (test length(rows) == 230)
    println("No obvious reduplication or missing detected")
end

```

## Iteration With Non-Uniform Ranges:

**SymbolRange** supports ::Function{SymbolRange, SymbolRange} rather than ::AbstractArray as range values.

*The order of the range dependence cannot be messed up!*

```julia
parameterOrder = [:w, :x, :y, :z] # the order of CSVInfo.keys is vital
dynamicParameterRanges = SymbolRange(
    :w => 1:10, # The range of the first parameter cannot be a function
    :x => d -> d[ :w ] : 10, # The range of x depends on w
    :y => d -> 1 : (d[ :x ] - d[ :w ] + 1), # The range of y depends on x and w
    :z => d -> d[ :y ] : d[ :y ] + 2 # The range of z depends on y
)
sample = (w = 1, x = 1, y = 1, z = 1)
# info = CSVInfo(sample, "sample.csv"; skeys = parameterOrder) # use skeys to store the order.
# OR:
info = CSVInfo( parameterOrder, dynamicParameterRanges, "sample.csv")
iterFromLast(f, dynamicParameterRanges, info) # function f takes up a parameter Dict and returns your data

```


## See Also:
```
/test/runtest.jl
```
