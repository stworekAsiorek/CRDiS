import numpy as np

def loadDataSet():
    return [[1, 1, 3, 4],
            [2, 3, 5, 1],
            [1, 1, 2, 3, 5],
            [2, 5]]


def createC1(dataSet):
    C1 = []
    for transaction in dataSet:
        for item in transaction:
            if not [item] in C1:
                C1.append([item])

    #C1.sort()
    #return list(map(frozenset, C1))
    return C1

def contains(small, big):
    for i in range(len(big)-len(small)+1):
        for j in range(len(small)):
            if big[i+j] != small[j]:
                break
        else:
            return i, i+len(small)
    return False

print(contains([1,3,4], [1, 3, 4]))


def scanD(D, Ck, minSupport):
    ssCnt = {}
    for tid in D:
        for can in Ck:
            if contains(can, tid):
                #print(can)
                if not tuple(can) in ssCnt:
                    ssCnt[tuple(can)] = 1
                else:
                    ssCnt[tuple(can)] += 1
    numItems = float(len(D))
    retList = []
    supportData = {}
    for key in ssCnt:
        support = ssCnt[key]/numItems
        if support >= minSupport:
            retList.append(key)
        supportData[key] = support
    return retList, supportData

dataSet = loadDataSet()
# print(dataSet)
#
C1 = createC1(dataSet)
# print(C1)
#
D = dataSet
#print(D)
#
L1,suppDat0 = scanD(D,C1,0.5)
print(L1)

def aprioriGen(Lk, k): #creates Ck
    retList = []
    lenLk = len(Lk)
    #print("Lk:", Lk)
    for i in range(lenLk):
        for j in range(lenLk):
            # L1 = list(Lk[i])[:k-2]
            # L2 = list(Lk[j])[:k-2]
            # print(L1)
            # print(L2)
            # #L1.sort()
            # #L2.sort()
            # if L1==L2: #if first k-2 elements are equal
            retList.append(Lk[i] + Lk[j]) #set union
            #print(retList)
    return retList

def apriori(dataSet, minSupport = 0.5):
    C1 = createC1(dataSet)
    D = dataSet
    L1, supportData = scanD(D, C1, minSupport)
    L = [L1]
    k = 2
    while (len(L[k-2]) > 0):
        Ck = aprioriGen(L[k-2], k)
        print("Cl:",Ck)
        Lk, supK = scanD(D, Ck, minSupport)#scan DB to get Lk
        print("Lk:",Lk)
        supportData.update(supK)
        L.append(Lk)
        k += 1
    return L, supportData

L,suppData = apriori(dataSet)
print(L)
print(suppData)

def generateRules(L, supportData, minConf=0.7):  #supportData is a dict coming from scanD
    bigRuleList = []
    for i in range(1, len(L)):#only get the sets with two or more items
        for freqSet in L[i]:
            H1 = [frozenset([item]) for item in freqSet]
            if (i > 1):
                rulesFromConseq(freqSet, H1, supportData, bigRuleList, minConf)
            else:
                calcConf(freqSet, H1, supportData, bigRuleList, minConf)
    return bigRuleList

def calcConf(freqSet, H, supportData, brl, minConf=0.7):
    prunedH = [] #create new list to return
    for conseq in H:
        conf = supportData[freqSet]/supportData[freqSet-conseq] #calc confidence
        if conf >= minConf:
            print (freqSet-conseq,'-->',conseq,'conf:',conf)
            brl.append((freqSet-conseq, conseq, conf))
            prunedH.append(conseq)
    return prunedH

def rulesFromConseq(freqSet, H, supportData, brl, minConf=0.7):
    m = len(H[0])
    if (len(freqSet) > (m + 1)): #try further merging
        Hmp1 = aprioriGen(H, m+1)#create Hm+1 new candidates
        Hmp1 = calcConf(freqSet, Hmp1, supportData, brl, minConf)
        if (len(Hmp1) > 1):    #need at least two sets to merge
            rulesFromConseq(freqSet, Hmp1, supportData, brl, minConf)

#L, suppData = apriori(dataSet, minSupport=0.5)
#rules= generateRules(L,suppData, minConf=0.7)