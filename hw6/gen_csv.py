prev_path="yago_simplified.txt"
new_path="yago_simplified.csv"

prev_file=open(prev_path,'r')
new_file=open(new_path,'w')

while True:
    raw_line=prev_file.readline()
    items=raw_line.strip().split()
    item_cnt=len(items)
    if(item_cnt<1):
        break
    s=items[0]
    p=items[1]
    o=items[2]
    new_file.write(s+","+p+","+o+"\n")
new_file.close()
prev_file.close()