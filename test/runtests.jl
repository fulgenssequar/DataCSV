using DataCSV, Test

sample = ( x = 5, y = 3 )
info = CSVInfo( sample, "./xy.csv" )
for  x in 1:20
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

for x in 1:2:30
    for y in 1:2:10
        key = (x = x, y = y)
        if (keyExists( key, oks))
            println("skipping $key   \r")
            continue
        end
        matrix = rand( x % 3 + 1, y % 4 + 1 )
        avg = sqrt( 0.5 * ( x ^ 2 + y ^ 2))
        data = Dict(:matrix => matrix, :avg => avg)
        dict2File( key, data, info )
        println("Key $key in storage      \r")
    end
end

rows2 = file2Rows(info)
sampleRow =   findRows( sample, rows2 )  

@test length(sampleRow) == 1

rm( "xy.csv" )

