
########### This part was written by Prabu ############
import pandas as pd

def humid(pc0_1, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0, humidity, temperature, dewPoint):
    pc0_1 = float(pc0_1)
    pc0_3 = float(pc0_3)
    pc0_5 = float(pc0_5)
    pc1_0 = float(pc1_0)
    pc2_5 = float(pc2_5)
    pc5_0 = float(pc5_0)
    pc10_0 = float(pc10_0)

    hum = float(humidity)
    tem = float(temperature)
    dew = float(dewPoint)
    T_D = tem - dew

    if hum > 40 and T_D < 2.5:
        print('Condition is satisfied')
        data = {'count': [pc0_1, None, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0, None],
            'D_range': [50, 20, 200, 200, 500, 1500, 2500, 5000, None],
            'D_point': [50, 80, 100, 300, 500, 1000, 2500, 5000, 10000]}
        df1 = pd.DataFrame(data)
        df1['N/D'] = df1['count']/df1['D_range']

        df1['height_ini'] = 0
        df1['height_ini'][7] = (2*df1['count'][7])/5000
        df1['height_ini'][6] = (2*df1['count'][6])/2500 - df1['height_ini'][7]
        df1['height_ini'][5] = (2*df1['count'][5])/1500 - df1['height_ini'][6]
        df1['height_ini'][4] = (2*df1['count'][4])/500 - df1['height_ini'][5]
        df1['height_ini'][3] = (2*df1['count'][3])/200 - df1['height_ini'][4]
        df1['height_ini'][2] = (2*df1['count'][2])/200 - df1['height_ini'][3]
        df1['height_ini'][0] = (2*df1['count'][0])/50 - df1['height_ini'][2]
        df1['height_ini'][1] = (20*(df1['height_ini'][0]-df1['height_ini'][2])/50) + df1['height_ini'][2]
        df1['count'][1] = 0.5*(df1['height_ini'][1]+df1['height_ini'][2])*20

        RH = (hum) * 0.7
        RH = 98 if RH >= 99 else RH
        k = 0.62
        df1['D_dry_point'] = df1['D_point']/((1 + k*(RH/(100-RH)))**(1/3))

        df1['D_dry_range'] = df1['D_dry_point'].diff().shift(-1)

        df1['fit_height_ini'] = 0
        df1['fit_height_ini'][7] = (2*df1['count'][7])/df1['D_dry_range'][7]
        df1['fit_height_ini'][6] = (2*df1['count'][6])/df1['D_dry_range'][6] - df1['fit_height_ini'][7]
        df1['fit_height_ini'][5] = (2*df1['count'][5])/df1['D_dry_range'][5] - df1['fit_height_ini'][6]
        df1['fit_height_ini'][4] = (2*df1['count'][4])/df1['D_dry_range'][4] - df1['fit_height_ini'][5]
        df1['fit_height_ini'][3] = (2*df1['count'][3])/df1['D_dry_range'][3] - df1['fit_height_ini'][4]
        df1['fit_height_ini'][2] = (2*df1['count'][2])/df1['D_dry_range'][2] - df1['fit_height_ini'][3]
        df1['fit_height_ini'][1] = (2*df1['count'][1])/df1['D_dry_range'][1] - df1['fit_height_ini'][2]

        df1['slope'] = (df1['fit_height_ini'].shift(-1) - df1['fit_height_ini']) / df1['D_dry_range']
        df1['interc'] = df1['fit_height_ini'] - df1['slope'] * df1['D_dry_point']

        df1['cor_height'] = None
        df1['cor_count'] = 0

        if df1['D_dry_point'][8] > 5000:
            df1['cor_height'][7] = df1['slope'][7]*5000 + df1['interc'][7]
            df1['cor_count'][7] = 0.5*df1['cor_height'][7]*(df1['D_dry_point'][8]-5000)
        else:
            df1['cor_height'][7] = 0
            df1['cor_count'][7] = 0
        
        if (2500<df1['D_dry_point'][7]<=5000)&(df1['D_dry_point'][8]>5000):
            df1['cor_height'][6] = df1['slope'][6]*2500 + df1['interc'][6]
            df1['cor_count'][6] = (0.5*(df1['cor_height'][7]+df1['fit_height_ini'][7])*(5000-df1['D_dry_point'][7])) + (0.5*(df1['cor_height'][6]+df1['fit_height_ini'][7])*(df1['D_dry_point'][7]-2500))
        elif (2500<df1['D_dry_point'][7]<5000)&(df1['D_dry_point'][8]<5000):
            df1['cor_height'][6] = df1['slope'][6]*2500 + df1['interc'][6]
            df1['cor_count'][6] = (0.5*(df1['cor_height'][6]+df1['fit_height_ini'][7])*(df1['D_dry_point'][7]-2500)) + (0.5*df1['fit_height_ini'][7]*(df1['D_dry_point'][8]-df1['D_dry_point'][7]))
        elif (df1['D_dry_point'][7]<2500)&(df1['D_dry_point'][8]<5000):
            df1['cor_height'][6] = df1['slope'][7]*2500 + df1['interc'][7]
            df1['cor_count'][6] = (0.5*df1['cor_height'][6])*(df1['D_dry_point'][8]-2500)
        else:
            df1['cor_height'][6] = df1['slope'][7]*2500 + df1['interc'][7]
            df1['cor_count'][6] = 0.5*(df1['cor_height'][7]+df1['cor_height'][6])*2500
        
        if (1000<df1['D_dry_point'][6]<=2500)&(df1['D_dry_point'][7]>2500):
            df1['cor_height'][5] = df1['slope'][5]*1000 + df1['interc'][5]
            df1['cor_count'][5] = (0.5*(df1['cor_height'][6]+df1['fit_height_ini'][6])*(2500-df1['D_dry_point'][6])) + (0.5*(df1['cor_height'][5]+df1['fit_height_ini'][6])*(df1['D_dry_point'][6]-1000))
        elif (1000<df1['D_dry_point'][6]<2500)&(df1['D_dry_point'][7]<2500):
            df1['cor_height'][5] = df1['slope'][5]*1000 + df1['interc'][5]
            df1['cor_count'][5] = (0.5*(df1['cor_height'][5]+df1['fit_height_ini'][6])*(df1['D_dry_point'][6]-1000)) + (0.5*(df1['fit_height_ini'][6]+df1['fit_height_ini'][7])*(df1['D_dry_point'][7]-df1['D_dry_point'][6])) + (0.5*(df1['fit_height_ini'][7]+df1['cor_height'][6])*(2500-df1['D_dry_point'][7]))
        elif (df1['D_dry_point'][6]<1000)&(df1['D_dry_point'][7]<2500):
            df1['cor_height'][5] = df1['slope'][6]*1000 + df1['interc'][6]
            df1['cor_count'][5] = (0.5*(df1['cor_height'][6]+df1['fit_height_ini'][7])*(2500-df1['D_dry_point'][7])) + (0.5*(df1['cor_height'][5]+df1['fit_height_ini'][7])*(df1['D_dry_point'][7]-1000))
        else:
            df1['cor_height'][5] = df1['slope'][6]*1000 + df1['interc'][6]
            df1['cor_count'][5] = 0.5*(df1['cor_height'][6]+df1['cor_height'][5])*1500

        if (500<df1['D_dry_point'][5]<=1000)&(df1['D_dry_point'][6]>1000):
            df1['cor_height'][4] = df1['slope'][4]*500 + df1['interc'][4]
            df1['cor_count'][4] = (0.5*(df1['cor_height'][5]+df1['fit_height_ini'][5])*(1000-df1['D_dry_point'][5])) + (0.5*(df1['cor_height'][4]+df1['fit_height_ini'][5])*(df1['D_dry_point'][5]-500))
        elif (500<df1['D_dry_point'][5]<1000)&(df1['D_dry_point'][6]<1000):
            df1['cor_height'][4] = df1['slope'][4]*500 + df1['interc'][4]
            df1['cor_count'][4] = (0.5*(df1['cor_height'][4]+df1['fit_height_ini'][5])*(df1['D_dry_point'][5]-500)) + (0.5*(df1['fit_height_ini'][5]+df1['fit_height_ini'][6])*(df1['D_dry_point'][6]-df1['D_dry_point'][5])) + (0.5*(df1['fit_height_ini'][6]+df1['cor_height'][5])*(1000-df1['D_dry_point'][6]))
        elif (df1['D_dry_point'][5]<500)&(df1['D_dry_point'][6]<1000):
            df1['cor_height'][4] = df1['slope'][5]*500 + df1['interc'][5]
            df1['cor_count'][4] = (0.5*(df1['cor_height'][5]+df1['fit_height_ini'][6])*(1000-df1['D_dry_point'][6])) + (0.5*(df1['cor_height'][4]+df1['fit_height_ini'][6])*(df1['D_dry_point'][6]-500))
        else:
            df1['cor_height'][4] = df1['slope'][5]*500 + df1['interc'][5]
            df1['cor_count'][4] = 0.5*(df1['cor_height'][5]+df1['cor_height'][4])*500

        if (300<df1['D_dry_point'][4]<=500)&(df1['D_dry_point'][5]>500):
            df1['cor_height'][3] = df1['slope'][3]*300 + df1['interc'][3]
            df1['cor_count'][3] = (0.5*(df1['cor_height'][4]+df1['fit_height_ini'][4])*(500-df1['D_dry_point'][4])) + (0.5*(df1['cor_height'][3]+df1['fit_height_ini'][4])*(df1['D_dry_point'][4]-300))
        elif (300<df1['D_dry_point'][4]<500)&(df1['D_dry_point'][5]<500):
            df1['cor_height'][3] = df1['slope'][3]*300 + df1['interc'][3]
            df1['cor_count'][3] = (0.5*(df1['cor_height'][3]+df1['fit_height_ini'][4])*(df1['D_dry_point'][4]-300)) + (0.5*(df1['fit_height_ini'][4]+df1['fit_height_ini'][5])*(df1['D_dry_point'][5]-df1['D_dry_point'][4])) + (0.5*(df1['fit_height_ini'][5]+df1['cor_height'][4])*(500-df1['D_dry_point'][5]))
        elif (df1['D_dry_point'][4]<300)&(df1['D_dry_point'][5]<500):
            df1['cor_height'][3] = df1['slope'][4]*300 + df1['interc'][4]
            df1['cor_count'][3] = (0.5*(df1['cor_height'][4]+df1['fit_height_ini'][5])*(500-df1['D_dry_point'][5])) + (0.5*(df1['cor_height'][3]+df1['fit_height_ini'][5])*(df1['D_dry_point'][5]-300))
        else:
            df1['cor_height'][3] = df1['slope'][4]*300 + df1['interc'][4]
            df1['cor_count'][3] = 0.5*(df1['cor_height'][4]+df1['cor_height'][3])*200

        if (100<df1['D_dry_point'][3]<=300)&(df1['D_dry_point'][4]>300):
            df1['cor_height'][2] = df1['slope'][2]*100 + df1['interc'][2]
            df1['cor_count'][2] = (0.5*(df1['cor_height'][3]+df1['fit_height_ini'][3])*(300-df1['D_dry_point'][3])) + (0.5*(df1['cor_height'][2]+df1['fit_height_ini'][3])*(df1['D_dry_point'][3]-100))
        elif (100<df1['D_dry_point'][3]<300)&(df1['D_dry_point'][4]<300):
            df1['cor_height'][2] = df1['slope'][2]*100 + df1['interc'][2]
            df1['cor_count'][2] = (0.5*(df1['cor_height'][2]+df1['fit_height_ini'][3])*(df1['D_dry_point'][3]-100)) + (0.5*(df1['fit_height_ini'][3]+df1['fit_height_ini'][4])*(df1['D_dry_point'][4]-df1['D_dry_point'][3])) + (0.5*(df1['fit_height_ini'][4]+df1['cor_height'][3])*(300-df1['D_dry_point'][4]))
        elif (df1['D_dry_point'][3]<100)&(df1['D_dry_point'][4]<300):
            df1['cor_height'][2] = df1['slope'][3]*100 + df1['interc'][3]
            df1['cor_count'][2] = (0.5*(df1['cor_height'][3]+df1['fit_height_ini'][4])*(300-df1['D_dry_point'][4])) + (0.5*(df1['cor_height'][2]+df1['fit_height_ini'][4])*(df1['D_dry_point'][4]-100))
        else:
            df1['cor_height'][2] = df1['slope'][3]*100 + df1['interc'][3]
            df1['cor_count'][2] = 0.5*(df1['cor_height'][3]+df1['cor_height'][2])*200

        if (50<df1['D_dry_point'][2]<=100)&(df1['D_dry_point'][3]>100):
            df1['cor_height'][0] = df1['slope'][1]*50 + df1['interc'][1]
            df1['cor_count'][0] = (0.5*(df1['cor_height'][2]+df1['fit_height_ini'][2])*(100-df1['D_dry_point'][2])) + (0.5*(df1['cor_height'][0]+df1['fit_height_ini'][2])*(df1['D_dry_point'][2]-50))
        elif (50<df1['D_dry_point'][2]<100)&(df1['D_dry_point'][3]>100):
            df1['cor_height'][0] = df1['slope'][1]*50 + df1['interc'][1]
            df1['cor_count'][0] = (0.5*(df1['cor_height'][0]+df1['fit_height_ini'][2])*(df1['D_dry_point'][2]-50)) + (0.5*(df1['fit_height_ini'][2]+df1['fit_height_ini'][3])*(df1['D_dry_point'][3]-df1['D_dry_point'][2])) + (0.5*(df1['fit_height_ini'][3]+df1['cor_height'][2])*(100-df1['D_dry_point'][3]))
        elif (df1['D_dry_point'][2]<50)&(df1['D_dry_point'][3]>100):
            df1['cor_height'][0] = df1['slope'][2]*50 + df1['interc'][2]
            df1['cor_count'][0] = (0.5*(df1['cor_height'][2]+df1['fit_height_ini'][3])*(100-df1['D_dry_point'][3])) + (0.5*(df1['cor_height'][0]+df1['fit_height_ini'][3])*(df1['D_dry_point'][3]-50))
        else:
            df1['cor_height'][0] = df1['slope'][2]*50 + df1['interc'][2]
            df1['cor_count'][0] = 0.5*(df1['cor_height'][2]+df1['cor_height'][0])*50

        pc0_1, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0 = df1['cor_count'][0], df1['cor_count'][2], df1['cor_count'][3], df1['cor_count'][4], df1['cor_count'][5], df1['cor_count'][6], df1['cor_count'][7]
    else:
        print('Condition is not satisfied')

    return pc0_1, pc0_3, pc0_5, pc1_0, pc2_5, pc5_0, pc10_0, hum, tem, dew

