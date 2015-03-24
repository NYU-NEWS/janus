

array = 1.step(20).to_a + 30.step(100, 10).to_a


mode = "rcc"

for i in array
  cmd = "./run.py -H ./config/hosts-teaker -f ./config/teaker-sosp/#{mode}_#{i}.xml &> #{mode}_#{i}.txt"
  value = `cmd`
end

mode = "ro6"

for i in array
  cmd = "./run.py -H ./config/hosts-teaker -f ./config/teaker-sosp/#{mode}_#{i}.xml &> #{mode}_#{i}.txt"
  value = `cmd`
end