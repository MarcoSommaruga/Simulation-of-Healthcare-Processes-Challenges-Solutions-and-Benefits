import warnings
import scipy.stats as st
import pytz
utc=pytz.UTC
import math
import numpy as np
import pandas as pd
from scipy import stats
from decimal import Decimal
import matplotlib.pyplot as plt


DISTRIBUTION={"norm": "TruncatedNormalDistribution", 
              "expon": "NegativeExponentialDistribution", 
              "uniform": "UniformDistribution", 
              "triang": "TriangularDistribution", 
              "user": "UserDistribution", 
              "truncnorm": "TruncatedNormalDistribution", 
              "gamma":"GammaDistribution"}

DISTRIBUTION_PARAMETER = {'BetaDistribution': ['shape', 'scale'], 
                          'BinomialDistribution' : ['probability', 'trials'],
                          'ErlangDistribution' : ['mean', 'k'], 
                          'GammaDistribution':['shape', 'scale'], 
                          'LogNormalDistribution':['mean', 'standardDeviation'], 
                          'NegativeExponentialDistribution':['mean'], 
                          'NormalDistribution':['mean', 'standardDeviation'], 
                          'PoissonDistribution':['mean'],
                          'TriangularDistribution':['mode', 'min', 'max'], 
                          'TruncatedNormalDistribution':['mean','standardDeviation','min','max'],
                          'UniformDistribution':['min','max'], 
                          'UserDistribution': ['points', 'discrete'], 
                          'WeibullDistribution':['shape','scale']}


class FitDistribution():

    def __init__(self, data):

        self.data=data
        self.bins=self.getBins() 
        self.data_without_outliers=self.delete_outliers(data)


    def find_parameter(self):

        flag, bin_edges = self.check_if_constant()
        if flag==True:
            return [0,0,False]  
        else:
            distribution=self.best_fit()
            if distribution[0]=='GammaDistribution':
                distribution[1]=self.paramsGamma(distribution[1])
            if distribution[0]=='NormalDistribution' or distribution[0]=='TruncatedNormalDistribution':
                minimum=0
                maximum=max(self.data[0])
                distribution[1]=distribution[1]+(minimum, maximum)
        return distribution
       

    def paramsGamma(self, params):

        shape=(params[0]**2)/(params[1]**2)
        scale=(params[1]**2)/params[0]
        return (shape, scale)


    def check_if_constant(self):

        hist,bin_edges=np.histogram(self.data,bins=self.bins)
        hist_treshold = 0.95 * np.sum(hist) 
        check_constant=False
        for idx, i in enumerate(hist):
            if i > hist_treshold: 
                check_constant=True
                break
        return check_constant, (bin_edges[idx], bin_edges[idx+1])       


    def getBins(self):

        q1=self.data.quantile([0.25], numeric_only=False) #find the first quantile of the data
        q3=self.data.quantile([0.75], numeric_only=False) #find the third quantile of the data
        if q3.iloc[0][0]==q1.iloc[0][0]:
            bins=10
        else:
            IQR= q3.iloc[0][0]-q1.iloc[0][0] 
            freedman_bins_width=(2*IQR)/(len(self.data)**(1/3)) 
            bins=math.ceil((self.data.max()-self.data.min())/freedman_bins_width) 
        return min(bins,20)


    def best_fit(self):

            best_distribution=st.norm 
            best_params = (0.0, 1.0)

            best_error = np.inf
            best_difference_area=np.inf
            flag=False

            PYTHON_DISTRIBUTIONS=[st.norm, st.expon,  st.uniform, st.truncnorm]
            Y,X = np.histogram(self.data, bins=self.bins, density=True)
            T=X[:-1]+ (X[2]-X[1])/2

            for distribution in PYTHON_DISTRIBUTIONS:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore') #Ignore warnings from data that can't be fit
                    params = distribution.fit(self.data[0])

                    arg = params[:-2]   
                    loc = params[-2]    
                    scale = params[-1]  

                    pdf = distribution.pdf(T, loc=loc, scale=scale, *arg)

                    sse = np.sqrt(np.sum(np.power(Y - pdf, 2.0)))
                    difference_area=np.sum(np.abs(Y - pdf)*(X[1]-X[0]))

                    if best_error > sse > 0:
                        best_distribution = distribution
                        best_params = params
                        best_error = sse
                        best_difference_area=difference_area
            
            if best_difference_area<=0.05:
                flag=True
            return [DISTRIBUTION[best_distribution.name], best_params, flag]


    
    def delete_outliers(self, data):

        q1=self.data.quantile([0.25], numeric_only=False).iloc[0,0]
        q3=self.data.quantile([0.75], numeric_only=False).iloc[0,0]
        IQR= q3-q1
        data=np.array([x for x in data.iloc[:,0]])
        new_data = {0:[]}
        idx = np.where(np.logical_and(data>(q1-1.5*IQR), data<(q3+1.5*IQR)))[0]
        for i in idx:
            new_data[0].append(data[i])
        return pd.DataFrame(new_data)


    def user_distribution(self, flag=0):

        hist_array, bin_array= np.histogram(self.data, bins=self.bins)
        value=[]
        probability=[]
        for i in range(0, len(bin_array)-1):
            freq=hist_array[i]/len(self.data)
            freq=round(freq, 3)
            if freq>0:
                if i+1==self.bins:
                    extract=self.data[(self.data[0] >= bin_array[i]) & (self.data[0] <= bin_array[i+1])]
                else:
                    extract=self.data[(self.data[0] >= bin_array[i]) & (self.data[0] < bin_array[i+1])]
                
                if np.mean(extract)[0]>=0:
                    value.append(np.mean(extract)[0])
                    probability.append(freq)

        probability[-1]=round((1-sum(probability[:-1])),3)
        new_probability=[]
        new_value=[]
        for prob_idx,prob in enumerate(probability):
            if prob>=0:
                new_probability.append(prob)
                new_value.append(value[prob_idx])

        if flag==1:
            return pd.DataFrame(data={'probability': new_probability, 'value': new_value}), self.data

        return pd.DataFrame(data={'probability': new_probability, 'value': new_value})