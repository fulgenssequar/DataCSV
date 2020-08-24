using DataCSV, Test

if (isfile("xy.csv"))
    rm("xy.csv")
end

sample = ( x = 5, y = 3 )
info = CSVInfo( sample, "./xy.csv" )
for  x in 1:10
    for y in 1:10
        matrix = rand( x % 3 + 1, y % 4 + 1 )
        avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
        dict2File( ( x = x, y = y ), Dict( :matrix  => matrix, :avg => avg ), info )
        print( " x = $x, y = $y stored         \r" )
    end
end

println()

@info " iterate selected rows from a csv file"
@test isfile("xy.csv")

rows = file2Rows( info )

# all matrices of size 3 x 4

mx34 = findRows( r -> r.x % 3 == 2 && r.y % 4 == 3, rows; iter = true )

sum34 = foldl( ( x, y) -> (x .+ y[ :matrix ]), mx34 ; init = 0)
display( sum34 )
println()

@info "select with map"
@test size( sum34 ) == ( 3, 4 )

# all matrices of size 2 x 3

mx23 = findRows( r -> r.x % 3 == 1 && r.y % 4 == 2, rows; iter = true, dmapper = d -> d[:matrix] )

sum23 = foldl( (x, y) -> x .+ y, mx23 )

display( sum23 )
println()
@test size(sum23) == (2, 3)

@info "get all keys"

oks = file2Keys( info; lazyList = true)

for x in 1:20
    for y in 1:10
        key = (x = x, y = y)
        if (keyExists( key, oks))
            println("skipping $key   \r")
            continue
        end
        matrix = rand( x % 3 + 1, y % 4 + 1 )
        avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
        data = Dict(:matrix => matrix, :avg => avg)
        dict2File( key, data, info )
        println("Key $key computed and stored \r")
    end
end

rows2 = file2Rows(info)
sampleRow =   findRows( sample, rows2 )  

@test length(sampleRow) == 1

@info "Testing the very delicate headFirst function ..."
function orderless(a1) 
    if (isempty(a1)) false
    else
        x, y = a1[1]
        if x < y true
        elseif x == y
            orderless(a1[2:end])
        else
            false
        end
    end
end


function f(t) 
    ab = [(t[m], 6) for m in [:w, :x, :y, :z]]
    orderless(ab)
end


pointers = [:w, :x, :y, :z]
ranges = Dict(:w=>0:7,:x=>2:8, :y=>0:13, :z=>7:9)

out = headFirst(pointers, ranges, f)
println(out)

@test  out[:w][1] == out[:x][1] == out[:y][1] == 6 && out[:z][1] == 7


@info "Check Advanced Iteration Method:"

iKeys = [:x, :y]
iRanges = Dict(
    :x => 1:23,
    :y => (d) -> (d[:x] < 21) ? (1 : 10) : (1 : d[:x] - 20)
)

function getData(paras)
    x = paras[ :x ]
    y = paras[ :y ]
    matrix = rand( x % 3 + 1, y % 4 + 1 )
    avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
    data = Dict(:matrix => matrix, :avg => avg)
    println("Smart Iteration at: x = $x, y = $y")
    data
end

iterForward(getData, iKeys, iRanges, info)

rows = collect(file2Rows(info))

@test length(rows) == 206

rm( "xy.csv" )

