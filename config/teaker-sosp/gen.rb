
require 'nokogiri'

f = File.open("template.xml")
doc = Nokogiri::XML(f)

clients  = doc.at_css "clients"

# puts clients['number']

client_elements = clients.elements

modes = ["ro6", "rcc", "2pl", "occ", "none"]

array = 1.step(20).to_a + 30.step(100, 10).to_a

benchmark = doc.at_css "benchmark"
for m in modes
  benchmark['mode'] = m
  for i in array
    clients['number'] = i * 8
    for j in 0..7
      element = client_elements[j]
      k1 = j * i
      k2 = j * i + (i - 1)
      element['id'] = "#{k1}-#{k2}"
    end
    ff = File.open("#{m}_#{i}.xml", "w")
    ff.write(doc)
    ff.close
  end
end

# clients.content.each do |c|
#   puts c
# end
# puts doc

