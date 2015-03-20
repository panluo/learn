#! /usr/bin/python
# -*- coding:utf-8 -*-

import sys
MIN_NUM=10

#读取文件内容
def getLines(startFilePath):
    f = open(startFilePath)
    lines = f.readlines()
    f.close()
    return lines

def main():
    lines = getLines(startFilePath)
    num = 0 #标记读取行的数量
    line1 = "0" #缓存，设置初始值为"0"
    fa = open(goalFilePath + "result_tanx.txt","w")
    fb = open(goalFilePath + "result_baidu.txt","w")
    fc = open(goalFilePath + "result_same.txt","w")
    for line in lines:
        #判断刚结束的是否为两行相同，如果是，则将此行放入缓存，读取下一行
        if line1 == "1":
            line1 = line
            continue
        line2 = line
        #读取第一行时的判断，将第一行放入缓存，读取下一行
        if line1 == "0":
            line1 = line2
            continue
        line1_d = line1.split("\t")
        line2_d = line2.split("\t")
        #将此行与上一行进行比较，IP不同，根据交易平台选择写入各自的文件中，并且将此行放入缓存：
        if line2_d[0] != line1_d[0]:
            print line1_d[2]
            print line1_d[1]
            if line1_d[3] == "tanx\n":
                if int(line1_d[2]) >= MIN_NUM or int(line1_d[1]) >= MIN_NUM:
                    fa.write(line1)
                    num = num + 1
            if line1_d[3] == "baidu\n":
                if int(line1_d[2]) >= MIN_NUM or int(line1_d[1]) >= MIN_NUM:
                    fb.write(line1)
                    num = num + 1
            line1 = line2
        #IP相同，同时将这两行写入文件，并将缓存置为"1"
        else:
            if (int(line1_d[1]) >= MIN_NUM or int(line2_d[1]) >= MIN_NUM) and \
                    (int(line1_d[2]) >= MIN_NUM or int(line2_d[2]) > MIN_NUM):
                fc.write(line1)
                fc.write(line2)
                num = num + 2
                line1 = "1"
    #防止漏掉文件中的最后一行数据
    if line1 != "1":
        line1_d = line1.splid("\t")
        if line1_d[3] == "tanx\n":
            fa.write(line1)
            num = num + 1
        if line1_d[3] == "baidu\n":
            fb.write(line1)
            num = num + 1

    fa.close()
    fb.close()
    fc.close()
    print "Complete"
    print num

if __name__ == "__main__":
    startFilePath = sys.argv[1]
    goalFilePath = sys.argv[2]
    main()
