import pandas as pd
from math import radians, atan, tan, sin, acos, cos, exp
def getDistance(latA, lonA, latB, lonB):  
    ra = 6378140  			# radius of equator: meter  
    rb = 6356755  			# radius of polar: meter  
    flatten = (ra - rb) / ra  	# Partial rate of the earth  
    # change angle to radians  
    radLatA = radians(latA)  
    radLonA = radians(lonA)  
    radLatB = radians(latB)  
    radLonB = radians(lonB)  

    try: 
        pA = atan(rb / ra * tan(radLatA))  
        pB = atan(rb / ra * tan(radLatB))  
        x = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(radLonA - radLonB))  
        c1 = (sin(x) - x) * (sin(pA) + sin(pB))**2 / cos(x / 2)**2  
        c2 = (sin(x) + x) * (sin(pA) - sin(pB))**2 / sin(x / 2)**2  
        dr = flatten / 8 * (c1 - c2)  
        distance = ra * (x + dr)  
        return distance			# meter   
    except:
        return 0.0000001
for k in [5, 10, 20, 30]:
    d = pd.read_csv('predict_result' + str(k) + '_7_day.csv')
    lt_500_count = 0
    total = 0
    score = 0
    max_n = -1
    for i in range(len(d)):
        val = getDistance(d.iloc[i, 1], d.iloc[i, 2], d.iloc[i, 3], d.iloc[i, 4])
        total += val
        score += (1 / (1 + exp((1000 - val) / 250)))
        if(val > max_n):
            max_n = val
        if(val < 500):
            lt_500_count += 1
        
        
    print(str(k), total / len(d), score / len(d), lt_500_count / len(d), lt_500_count)
