using DataCSV, Test

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
println( " Raw range: $rgF")
println( " Last data: $rgT" )
println( " Next data; $nxtRg  \n which is $status\n" )

info = CSVInfo([:w, :x, :y, :z], rgF, "onlykeys.csv")
doprint(d) = begin
    if d[:x] == d[:w] && d[:y] == d[:z]
        println()
    end
    print(" Iter->")
    for k in info.keys
        print("$k=$(d[k]);")
    end
    "nothing"
end
iterFromLast(doprint, rgF, info)
println()
@info "The last iteration was normal"
rm(info.fileName)



@info "Manual check: Another Example:"

sample = (repeat="time_1", x = BigInt(1), y = BigInt(2), time = BigInt(0))
info = CSVInfo(sample, "reminders.csv"; skeys = [:x, :y, :repeat, :time])
iKeys = [:repeat, :x, :y]
iRanges = SymbolRange(:repeat => map(i -> "repeat_$i", 1:5), :x => 1:10000, :y => d -> d[:x] : 10000)

rgLast = SymbolRange(
  :y      => 3968:10000,
  :repeat => ["repeat_1", "repeat_2", "repeat_3", "repeat_4", "repeat_5"],
  :x      => 985:10000
)
rgInit, status = DataCSV.getNextKey( iKeys, iRanges, rgLast )

println( "rgOrig = $iRanges")
println( "rgLast = $rgLast")
println( "rgInit = $rgInit")


@info "Check Advanced Iteration Method:"

sample = ( x = 5, y = 3, z = 1.5 )
info = CSVInfo( sample, "xy.csv" ; skeys = [:x, :y, :z])

if (isfile(info.fileName))
    rm(info.fileName)
end


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

iterForward(getData, iRanges1, info)

# get last iteration method 1 ( slow ):
checker = DataCSV.getRapidChecker(info)
init = DataCSV.headFirst(iKeys, iRanges2, checker)

# get last iteration method 2 ( directly ):
rLast = DataCSV.readLastLine( info )
iterLast = DataCSV.getLastKey( iKeys, iRanges2, rLast )
iterInit, status = DataCSV.getNextKey( iKeys, iRanges2, iterLast )

println()
println("Last run interrupted  before: $init\n")
println("So the computation resumes @: $iterInit\n")

@test isequal(iterInit, init)


println()
@info "Computer rebooted and preceeded"

checker = DataCSV.getRapidChecker(info)
@test checker((x=5, y=10, z=0.0))
@test ! checker((x=5, y=11, z=3.0))

iterForward(getData, iRanges2, info)

println()
iterFromLast( getData, iRanges3, info )

println()
@info "Checking total lines:"
rows = collect(file2Rows(info))
@test length(rows) == 564


println()
@info "Select with map"

# all matrices of size 2 x 3

mx23 = findRows( r -> r.x % 3 == 1 && r.y % 4 == 2, info; iter = true, nokeys = true, dmapper = d -> d[:matrix] )
println(collect(Iterators.take(mx23, 5)))
sum23 = foldl( (x, y) -> x .+ y, mx23 )

display( sum23 )
println()
@test size(sum23) == (2, 3)

rm( info.fileName )

