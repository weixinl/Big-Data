prev_data_path="yago_simplified.txt"
new_data_path="yago_2.txt"
matched_s_path="matched_s.txt"

prev_data_file=open(prev_data_path,'r')
new_data_file=open(new_data_path,'w')
matched_s_file=open(matched_s_path,'r')

matched_s_list=[]
while True:
    raw_line=matched_s_file.readline()
    items=raw_line.strip().split()
    item_cnt=len(items)
    if(item_cnt<1):
        break
    matched_s_list.append(items[0])
    

while True:
    raw_line=prev_data_file.readline()
    items=raw_line.strip().split()
    item_cnt=len(items)
    if item_cnt<1:
        break
    line_needed=False
    s=items[0]
    p=items[1]
    o=items[2]
    if(s=="David_Kaiser"or s=="Alexander_Hamilton_Jr"):
        line_needed=True
    if(o=="Eidak"):
        line_needed=True
    # if(p=="isLeaderOf"):
    #     line_needed=True
    # if(p=="owns"):
    #     line_needed=True
    if(s in matched_s_list):
        line_needed=True
    # if(o=="United_States"):
    #     line_needed=True
    if(line_needed):
        new_data_file.write(raw_line)

prev_data_file.close()
new_data_file.close()
