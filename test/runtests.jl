using DataCSV, Test

sample = ( x = 5, y = 3, z = 1.5 )
info = CSVInfo( sample, "xy.csv" ; skeys = [:x, :y, :z])

if (isfile(info.fileName))
    rm(info.fileName)
end


# for  x in 1:10
#     for y in 1:10
#         matrix = rand( x % 3 + 1, y % 4 + 1 )
#         avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
#         dict2File( ( x = x, y = y ), Dict( :matrix  => matrix, :avg => avg ), info )
#         print( " x = $x, y = $y stored         \r" )
#     end
# end

# println()

# @info " iterate selected rows from a csv file"
# @test isfile("xy.csv")

# rows = file2Rows( info )

# # all matrices of size 3 x 4

# mx34 = findRows( r -> r.x % 3 == 2 && r.y % 4 == 3, rows; iter = true )

# sum34 = foldl( ( x, y) -> (x .+ y[ :matrix ]), mx34 ; init = 0)
# display( sum34 )
# println()

# @info "get all keys"

# oks = file2Keys( info; lazyList = true)

# for x in 1:20
#     for y in 1:10
#         key = (x = x, y = y)
#         if (keyExists( key, oks))
#             println("skipping $key   \r")
#             continue
#         end
#         matrix = rand( x % 3 + 1, y % 4 + 1 )
#         avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
#         data = Dict(:matrix => matrix, :avg => avg)
#         dict2File( key, data, info )
#         println("Key $key computed and stored \r")
#     end
# end

# rows2 = file2Rows(info)
# sampleRow =   findRows( sample, rows2 )  

# @test length(sampleRow) == 1

# @info "Testing the very delicate headFirst function ..."
# function orderless(a1) 
#     if (isempty(a1)) false
#     else
#         x, y = a1[1]
#         if x < y true
#         elseif x == y
#             orderless(a1[2:end])
#         else
#             false
#         end
#     end
# end


# function f(t) 
#     ab = [(t[m], 6) for m in [:w, :x, :y, :z]]
#     orderless(ab)
# end


# pointers = [:w, :x, :y, :z]
# ranges = Dict(:w=>0:7,:x=>2:8, :y=>0:13, :z=>7:9)

# out = headFirst(pointers, ranges, f)
# println(out)

# @test  out[:w][1] == out[:x][1] == out[:y][1] == 6 && out[:z][1] == 7

SymbolRange = Dict{Symbol, Union{Function, <: AbstractArray}}
@info "Manual check: Whether next is true"
rgF = SymbolRange(
    :w => 1:10,
    :x => d -> d[ :w ] : 10,
    :y => d -> 1 : (d[ :x ] - d[ :w ] + 1),
    :z => d -> d[ :y ] : d[ :y ] + 2
)

rgT = SymbolRange(
    :w => 8 : 10,
    :x => 10:10,
    :y => 1:1,
    :z => 3:3
)

nxtRg , status = DataCSV.getNextKey([:w, :x, :y, :z], rgF, rgT)
println( " Last data: $rgT" )
println( " Next data; $nxtRg  \n which is $status" )


@info "Check Advanced Iteration Method:"

iKeys = [:x, :y, :z]

iRanges1 = SymbolRange(
    :x => 1:5,
    :y => 1:10,
    :z => -3.0 : 3.0 : 3.0
)

iRanges2 = SymbolRange(
    :x => 1:8,
    :y => (d) -> (d[:x] < 5) ? (1 : 10) : (1 : d[:x] + 10),
    :z => -3.0 : 3.0 : 3.0
)

iRanges3 = SymbolRange(
    :x => 1:12,
    :y => (d) -> (d[:x] < 5) ? (1 : 10) : (1 : d[:x] + 10),
    :z => -3.0 : 3.0 : 3.0
)

function getData(paras)
    x = paras[ :x ]
    y = paras[ :y ]
    z = paras[ :z ]
    matrix = rand( x % 3 + 1, y % 4 + 1 ) .+ z
    avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
    data = Dict(:matrix => matrix, :avg => avg)
    print(" :->", (x, y, z))
    data
end

@info "First computation precession:"

iterForward(getData, iKeys, iRanges1, info)

# get last iteration method 1 ( slow ):
checker = DataCSV.getRapidChecker(info)
init = DataCSV.headFirst(iKeys, iRanges2, checker)

# get last iteration method 2 ( directly ):
rLast = DataCSV.readLastLine( info )
iterLast = DataCSV.getLastKey( iKeys, iRanges2, rLast )
iterInit = DataCSV.getNextKey( iKeys, iRanges2, iterLast )

println()
println("Last run interrupted  before: $init\n")
println("So the computation resumes @: $iterInit\n")

@test isequal(iterInit, init)


println()
@info "Computer rebooted and preceeded"

checker = DataCSV.getRapidChecker(info)
@test checker((x=5, y=10, z=0.0))
@test ! checker((x=5, y=11, z=3.0))



iterForward(getData, iKeys, iRanges2, info)

println()
iterFromLast( getData, iKeys, iRanges3, info )

println()
@info "Checking total lines:"
rows = collect(file2Rows(info))
@test length(rows) == 564


println()
@info "Select with map"

# all matrices of size 2 x 3

mx23 = findRows( r -> r.x % 3 == 1 && r.y % 4 == 2, rows; iter = true, dmapper = d -> d[:matrix] )

sum23 = foldl( (x, y) -> x .+ y, mx23 )

display( sum23 )
println()
@test size(sum23) == (2, 3)

rm( info.fileName )

