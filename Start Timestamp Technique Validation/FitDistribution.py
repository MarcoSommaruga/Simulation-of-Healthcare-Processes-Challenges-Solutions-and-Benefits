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
        #self.data=data
        self.data=data
        #self.data_without_outliers=self.delete_outliers(data)

        self.bins=self.getBins() #return the number of bins for the istogram
        self.data_without_outliers=self.delete_outliers(data)

    def find_parameter(self):
        flag, bin_edges = self.check_if_constant()
        if flag==True: # if data are constant check_if_constant() returns true
            #print('\n\n LA DISTRIBUZIONE È COSTANTE')
            #print('BIN EDGES:', bin_edges[0], bin_edges[1])
            #return ['Constant', int(np.mean(self.data))]
            #se è costante ritorno una user_distribution
            return [0,0,False]  #se è costante calcolo una user_distribution
            #print(np.min(self.data), np.max(self.data))
            #return  ['Constant', int(np.mean(self.data)), int(np.min(self.data)), int(np.max(self.data))]  ### mettere una uniforme da min a max  
        else:
            distribution=self.best_fit() #it returns the best fitting distribution for the data 
            if distribution[0]=='GammaDistribution':
                distribution[1]=self.paramsGamma(distribution[1])
            if distribution[0]=='NormalDistribution' or distribution[0]=='TruncatedNormalDistribution':
                minimum=0
                maximum=max(self.data[0])
                ## MIN MAX  LOC SCALE
                distribution[1]=distribution[1]+(minimum, maximum)
        return distribution
       
    def paramsGamma(self, params):
        shape=(params[0]**2)/(params[1]**2)
        scale=(params[1]**2)/params[0]
        return (shape, scale)

    # VERSION OF THE FUNCTION WHERE IF I FIND A CONSTANT DISTRIBUTION I OUTPUT A UNIFORM DISTRIBUTION
    # IN THIS CASE I HAVE TO FIX ALSO THE find_parameters FUNCTION
    def check_if_constant(self): #it checks if the data are constant
        #alternativa: controllare se standard deviation è piccola
        
        hist,bin_edges=np.histogram(self.data,bins=self.bins)
        hist_treshold = 0.95 * np.sum(hist) #compute the sum of all the values of the bar of the histogram multiplied by 0.95 (to have a little of admission). hist-->variable that contains how many data there are in a bin. 
        #for example: data = [2,3,2,2,2,3,2], hist (with 4 bins [1.5, 2, 2.5, 3, 3.5]) = [5, 0, 2, 0] because in [1.5,2] there are 5 elements, in [2,2.5] 0 elements, in [2.5,3] 2 elements and in [3,3.5] 0 elements
        #computing 0.95*sum(hist)=0.95*7=6.65
        check_constant=False
        for idx, i in enumerate(hist): #for each number of elements in hist (in ower case [5,0,2,0]
            if i > hist_treshold: #in ower case 6.65
                check_constant=True
                break
        return check_constant, (bin_edges[idx], bin_edges[idx+1])       

    #def check_if_constant(self): #it checks if the data are constant   
    #    #alternativa: controllare se standard deviation è piccola
    #     
    #    hist,bin_edges=np.histogram(self.data,bins=self.bins)
    #    hist_treshold = 0.95 * np.sum(hist) #compute the sum of all the values of the bar of the histogram multiplied by 0.95 (to have a little of admission). hist-->variable that contains how many data there are in a bin. 
    #    #for example: data = [2,3,2,2,2,3,2], hist (with 4 bins [1.5, 2, 2.5, 3, 3.5]) = [5, 0, 2, 0] because in [1.5,2] there are 5 elements, in [2,2.5] 0 elements, in [2.5,3] 2 elements and in [3,3.5] 0 elements
    #    #computing 0.95*sum(hist)=0.95*7=6.65
    #    check_constant=False
    #    for i in hist: #for each number of elements in hist (in ower case [5,0,2,0]
    #        if i > hist_treshold: #in ower case 6.65
    #            check_constant=True
    #            break
    #    return check_constant

    def getBins(self): #it returns the number of bins of the histogram:
        #if the data are constant: it creates 10 bins
        #if the data are not constant: it computes the inter-quantile range and exploit the Freedman-Diaconis rule to compute the number of bins
        q1=self.data.quantile([0.25], numeric_only=False) #find the first quantile of the data
        q3=self.data.quantile([0.75], numeric_only=False) #find the third quantile of the data
        if q3.iloc[0][0]==q1.iloc[0][0]: #if the first and third quantile of the data are equal, then I create 10 bins
            bins=10
        else:
            IQR= q3.iloc[0][0]-q1.iloc[0][0] #compute the inter-quantile range (difference between the third and first quantile)
            freedman_bins_width=(2*IQR)/(len(self.data)**(1/3)) #by the Freedman-Diaconis rule, the number of bins k in an histogram should be ~n^(1/3), where n is the sample size. So, the width of each bin is computed as: (2*IQR)/n^(1/3)
            bins=math.ceil((self.data.max()-self.data.min())/freedman_bins_width) #math.ceil rounds the numbers
        return min(bins,20)


    def best_fit(self):
            best_distribution=st.norm 
            best_params = (0.0, 1.0)

            best_error = np.inf
            best_difference_area=np.inf
            flag=False

            PYTHON_DISTRIBUTIONS=[st.norm, st.expon,  st.uniform, st.truncnorm]
            Y,X = np.histogram(self.data, bins=self.bins, density=True) #Y-->values of the histogram, X-->bins of the histogram
            T=X[:-1]+ (X[2]-X[1])/2


            for distribution in PYTHON_DISTRIBUTIONS:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore') #Ignore warnings from data that can't be fit
                    params = distribution.fit(self.data[0])
                    ##return the maximum likelihood estimated parameters for the given distribution

                    arg = params[:-2]   # Separate parts of parameters
                    loc = params[-2]    ## media
                    scale = params[-1]  # varianza

                    pdf = distribution.pdf(T, loc=loc, scale=scale, *arg)

                    sse = np.sqrt(np.sum(np.power(Y - pdf, 2.0))) #sum of squares
                    difference_area=np.sum(np.abs(Y - pdf)*(X[1]-X[0]))

                    if best_error > sse > 0:
                        best_distribution = distribution
                        best_params = params
                        best_error = sse
                        best_difference_area=difference_area
            
            if best_difference_area<=0.40:
                flag=True
            #print('FLAG fit distribution', flag)
            #print("LOC ", best_params[-2])
            #print("SCALE", best_params[-1])
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


    #funzione da chiamare se la fit distribution è False

    ### UserDistribution with MEAN value
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
                
                #probability.append(round(freq,3))
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

        # in case of alpha_configuration I need to have the bin_array
        if flag==1:
            return pd.DataFrame(data={'probability': new_probability, 'value': new_value}), self.data

        return pd.DataFrame(data={'probability': new_probability, 'value': new_value})