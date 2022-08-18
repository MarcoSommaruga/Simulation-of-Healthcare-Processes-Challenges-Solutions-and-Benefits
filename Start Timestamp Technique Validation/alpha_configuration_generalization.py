# Classe che utiliza un algoritmo greedy per trovare gli alpha
from datetime import timedelta
import pandas as pd
from datetime import datetime,timedelta
import scipy.stats as st
import matplotlib.pyplot as plt
import pytz
utc=pytz.UTC
import numpy as np
import os
import pm4py
import xml.etree.ElementTree as ET
from lxml import etree, objectify
from collections import defaultdict
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import sorting
from SimulationColombia import SimulationBPSIM_Colombia
from SimulationPurchasing import SimulationBPSIM_Purchasing
import time
from FitDistribution import FitDistribution
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from statistics import mode

####### FARE ADD START SENZA RISORSE PER PISA ##############

BPMN2='{http://www.omg.org/spec/BPMN/20100524/MODEL}'


class alpha_configuration:
    # log colombiano
    def __init__(self, path_log, path_bpmn, alpha_start, log_name, start_activities = [], parallel_activities = [], needEndActivities = [], already_timestamped = []): # path_log_real.xes, path_bpmn.xml, alpha, start_activities (list with the start activities), parallel_activities (list with the parallel activities)
        if log_name == 'Colombia':
            self.log=xes_importer.apply(path_log) # real log
            self.path_bpmn=path_bpmn
            self.getTaskBPMN()
            self.alpha={'Traer informacion estudiante - banner': 0.0,'Revisar curso': 0.0, 'Cancelar Solicitud': 0.0, 'Evaluacion curso': 0.0, 'Homologacion por grupo de cursos': 0.0,
            'Notificacion estudiante cancelacion soli': 0.0, 'Validar solicitud': 0.0,'Radicar Solicitud Homologacion': 0.0}
            self.copy_alpha(alpha_start)
            self.start_activities = start_activities 
            self.parallel_activities = parallel_activities 
            self.needEndActivities = needEndActivities
            self.already_timestamped = already_timestamped
            self.log_name = log_name
        # log purchasing
        elif log_name == 'Purchasing':
            self.log=xes_importer.apply(path_log) 
            self.path_bpmn=path_bpmn
            self.getTaskBPMN()
            self.alpha={'Choose best option': 0.0, 'Settle Conditions With Supplier': 0.0, 'Create Purchase Order': 0.0, 'Send Invoice': 0.0, 'Confirm Purchase Order': 0.0, 'Deliver Goods Services': 0.0,
                        'Approve Purchase Order for payment': 0.0, 'Amend Request for Quotation': 0.0, 'Authorize Supplier\'s Invoice payment': 0.0, 'Settle Dispute With Supplier': 0.0, 'Release Purchase Order': 0.0,
                        'Analyze Purchase Requisition': 0.0, 'Send Request for Quotation to Supplier': 0.0, 'Pay Invoice': 0.0, 'Create Purchase Requisition': 0.0, 'Analyze Quotation Comparison Map': 0.0, 
                        'Analyze Request for Quotation': 0.0, 'Release Supplier\'s Invoice': 0.0, 'Create Request for Quotation': 0.0, 'Create Quotation comparison Map': 0.0}
            self.copy_alpha(alpha_start)
            self.start_activities = start_activities
            self.parallel_activities = parallel_activities 
            self.needEndActivities = needEndActivities
            self.already_timestamped = already_timestamped
            self.log_name = log_name
        
        
    def copy_alpha(self, alpha_start): 
        for idx, key in enumerate(self.alpha):
            self.alpha[key]=alpha_start[idx]
        print('\n\n\n\nALPHA:', self.alpha)



    def addStartTimestampAlphaRisorse(self, alpha_log, alpha):

        # add start:timestamp to the log

        dic=dict()
        for case_index, case in enumerate(alpha_log):
            for event_id,event in enumerate(case):
                if event['concept:name'] not in self.needEndActivities:
                    if event['concept:name'] in self.start_activities or event['concept:name'] not in self.alpha.keys():
                        event["start:timestamp"]=event['time:timestamp']
                    else:
                        maximum=event["time:timestamp"]
                        minimum=event["time:timestamp"]
                        if event_id>0:
                            if event['org:resource'] not in dic:
                                minimum=case[event_id-1]['time:timestamp']
                            else:
                                minimum=max(case[event_id-1]['time:timestamp'],dic[event['org:resource']] )
                        maximum=time.mktime(maximum.timetuple()) ### trasformo in secondi totali
                        minimum=time.mktime(minimum.timetuple()) ### trasformo in secondi totali
                        event["start:timestamp"]= (1-alpha[event['concept:name']]) * maximum + alpha[event['concept:name']] * minimum
                        event["start:timestamp"]= (datetime.fromtimestamp(event["start:timestamp"]))
                        dic[event['org:resource']]=event['time:timestamp']
            return alpha_log


    def addStartTimestampAlpha(self, alpha_log, alpha): #OK
        
        # add start:timestamp to the log

        for case_index, case in enumerate(alpha_log):
            for event_id,event in enumerate(case):
                if event['concept:name'] not in self.needEndActivities:
                    if event['concept:name'] in self.start_activities or event['concept:name'] not in self.alpha.keys():
                        event["start:timestamp"]=event['time:timestamp']
                    else:
                        maximum=event["time:timestamp"]
                        minimum=case[event_id-1]['time:timestamp']
                        maximum=time.mktime(maximum.timetuple()) ### trasformo in secondi totali
                        minimum=time.mktime(minimum.timetuple()) ### trasformo in secondi totali
                        event["start:timestamp"]= (1-alpha[event['concept:name']]) * maximum + alpha[event['concept:name']] * minimum
                        event["start:timestamp"]= (datetime.fromtimestamp(event["start:timestamp"]))
        return alpha_log


    
    def addStartTimestampAlphaRisorse_2(self, alpha_log, alpha):
        dic=dict()
        for case_index, case in enumerate(alpha_log):
            for event_id,event in enumerate(case):
                if event['concept:name'] in self.start_activities or event['concept:name'] not in self.alpha.keys():
                    event["start:timestamp"]=event['time:timestamp']
                else:
                    maximum=event["time:timestamp"]
                    minimum=event["time:timestamp"]
                    if event_id>0:
                        idx = 1
                        # if the task is performed in parallel with other activities, go back of another index to get the previous task
                        while case[event_id-idx]['concept:name'] in self.parallel_activities:
                            idx = idx + 1                        
                        if event['org:resource'] not in dic:
                            minimum = case[event_id-idx]['time:timestamp']
                        else:
                            if dic[event['org:resource']].replace(tzinfo=utc)<=event["time:timestamp"].replace(tzinfo=utc):
                                minimum = max(case[event_id-idx]['time:timestamp'],dic[event['org:resource']])
                    maximum=time.mktime(maximum.timetuple()) ### trasformo in secondi totali
                    minimum=time.mktime(minimum.timetuple()) ### trasformo in secondi totali
                    event["start:timestamp"]= (1-alpha[event['concept:name']]) * maximum + alpha[event['concept:name']] * minimum
                    event["start:timestamp"]= (datetime.fromtimestamp(event["start:timestamp"]))
                    dic[event['org:resource']]=event['time:timestamp']
                
        return alpha_log


    # add the start timestamp considering the previous activity that is not in parallel
    def addStartTimestampAlpha_2(self, alpha_log, alpha): #OK
        
        # add start:timestamp to the log

        for case_index, case in enumerate(alpha_log):
            for event_id,event in enumerate(case):
                if event['concept:name'] not in self.needEndActivities:
                    if event['concept:name'] in self.start_activities or event['concept:name'] not in self.alpha.keys():
                        event["start:timestamp"]=event['time:timestamp']
                    else:
                        maximum=event["time:timestamp"]
                        idx = 1
                        # if the task is performed in parallel with other activities, go back of another index to get the previous task
                        while alpha_log[case_index][event_id-idx]['concept:name'] in self.parallel_activities:
                            idx = idx + 1 
                        minimum=case[event_id-idx]['time:timestamp']
                        maximum=time.mktime(maximum.timetuple()) ### trasformo in secondi totali
                        minimum=time.mktime(minimum.timetuple()) ### trasformo in secondi totali
                        event["start:timestamp"]= (1-alpha[event['concept:name']]) * maximum + alpha[event['concept:name']] * minimum
                        event["start:timestamp"]= (datetime.fromtimestamp(event["start:timestamp"])) #### riconverto in datetime        
        return alpha_log



    # add the end timestamp considering the previous activity that is not in parallel
    def addEndTimestampAlphaRisorse_2(self, alpha_log, alpha):

        # IMPORTANT: RUN THIS AFTER HAVING RUN addStartTimestampAlphaRisorse_2
        # add time:timestamp to the log
        
        dic=dict()
        for case_index, case in enumerate(alpha_log):
            for event_id, event in enumerate(case):
                if event['concept:name'] in self.needEndActivities:
                    minimum = event['start:timestamp'] #REMEMBER TO PASS THE ACTIVITY THAT HAS NOT THE END TIMESTAMP, WITH THE START:TIMESTAMP
                    maximum = event['start:timestamp']
                    if event_id>0 and event_id+1<len(case):
                        idx = 1
                        flag = 0
                        # if the task is performed in parallel with other activities, go up of another index to get the task after
                        while case[event_id+idx]['concept:name'] in self.parallel_activities:
                            idx = idx + 1
                            # if event_id+idx is higher than the length of the case it means that the activity has not other (or it has just parallel) activities after itself
                            # so it can end "whenever it wants"
                            if event_id+idx >= len(case):
                                maximum = datetime(year=2099, month=12, day=31)
                                flag = 1
                                break                        
                        if event['org:resource'] not in dic and flag == 0:
                            maximum = case[event_id+idx]['start:timestamp']
                        elif event['org:resouce'] in dic and flag == 0:
                            maximum = max(case[event_id+idx]['start:timestamp'],dic[event['org:resource']])
                    maximum=time.mktime(maximum.timetuple()) ### trasformo in secondi totali
                    minimum=time.mktime(minimum.timetuple()) ### trasformo in secondi totali
                    event["time:timestamp"]= (1-alpha[event['concept:name']]) * maximum + alpha[event['concept:name']] * minimum
                    event["time:timestamp"]= (datetime.fromtimestamp(event["time:timestamp"]))
                    dic[event['org:resource']]=event['start:timestamp']
        return alpha_log


    #############################################################################    
    ########################VERSIONE SENZA DISTRIBUZIONI#########################
    #############################################################################   
    def errore_claudia(self, simulated_log): 
        trace_duration_distance= self.getTraceDurationDistance(simulated_log)
        waiting_time_distance=0
        for task in self.tasks:
            if task not in self.start_activities:
                waiting_time_distance+= self.getWaitingTimeDistance(simulated_log, task)
        return trace_duration_distance + waiting_time_distance

    #############################################################################    
    ##############VERSIONE ERRORE CON WAITING TIME + TRACE DURATION##############
    #############################################################################
    def errore_waiting_trace(self, simulated_log):

        # for each activity in the log (and also in the simulated_log) compute the error on the activity duration.
        # To do it, it has to be considered the distribution of the acitivity's durations both for the real and the simulated_log
        # and compute the error.
        # In particular, in this case it is considered the total trace duration and the waiting time duration for each task
        
        #real_activity_distributions = self.getActivityDistribution(self.log) #real_activity_distributions = {'task1':...}
        #simulated_activity_distributions = self.getActivityDistribution(simulated_log) #simulated_activity_distributions = {'task1':...}
        #task_error = {}

        #for task in self.tasks:
            #task_error[task] = self.computeRiemannDifference(real_activity_distributions[task], simulated_activity_distributions[task])
            #waiting_task_error[task] = self.computeRiemannDifference(waiting_real_distributions[task], waiting_simulated_distributions[task])
                            
        waiting_task_error = {}

        total_error = 0
        
        # compute the trace duration distribution of the real log
        print('****COMPUTING TRACE DISTRIBUTIONS****')
        trace_real_distribution, real_median_trace_duration = self.getTraceDistribution(self.log)
        print('REAL TRACE DISTRIBUTION\n', trace_real_distribution)
        print('MEDIANA TRACE DURATION REAL:', real_median_trace_duration)

        # compute the trace duration distribution of the simulated log
        trace_simulated_distribution, simulated_median_trace_duration = self.getTraceDistribution(simulated_log)
        print('SIMULATED TRACE DISTRIBUTION\n', trace_simulated_distribution)
        print('MEDIANA TRACE DURATION SIMULATED:', simulated_median_trace_duration)
        
        # compute the integral Riemann difference between the two distributions of each actibity (real and simulated)
        print('\n\n****COMPUTING RIEMANN DIFFERENCE TRACE DISTRIBUTIONS****\n\n')
        trace_duration_error = self.computeRiemannDifference(trace_real_distribution, trace_simulated_distribution)

        # compute the waiting time distribution of the real log
        print('\n****COMPUTING REAL WAITING DISTRIBUTIONS****\n')
        # here I receive as output also the dictionary real_waiting_medians where there are saved all the median about the waiting time for each task, so that
        # I can extract the median value to weight the error referred to that specific task and give more or less importance in the computation of the error
        waiting_real_distributions, real_waiting_medians = self.getWaitingDistribution(self.log) 
        print('MEDIANA WAITING DURATION REAL:', real_waiting_medians)

        # compute the waiting time distribution of the simulated log
        print('\n****COMPUTING SIMULATED WAITING DISTRIBUTIONS****\n')
        waiting_simulated_distributions, simulated_waiting_medians = self.getWaitingDistribution(simulated_log)
        print('MEDIANA WAITING DURATION SIMULATED:', simulated_waiting_medians)

        
        for task in self.tasks:
            #task_error[task] = self.computeRiemannDifference(real_activity_distributions[task], simulated_activity_distributions[task])
            # compute the integral Riemann difference between the two distributions of each activity (real and simulated)
            if task not in self.start_activities:
                print('\n\nCOMPUTE RIEMANN DIFFERENCE OF TASK:', task)
                print('REAL WAITING DISTRIBUTION', waiting_real_distributions[task])
                print('SIMULATED WAITING DISTRIBUTION', waiting_simulated_distributions[task])
                waiting_task_error[task] = self.computeRiemannDifference(waiting_real_distributions[task], waiting_simulated_distributions[task])

        # compute the total_error as the sum of all the waiting error and the trace duration error
        for task in self.tasks:
            if task not in self.start_activities:
                total_error +=  waiting_task_error[task] * np.abs(real_waiting_medians[task]-simulated_waiting_medians[task]) # weight the error on each task waiting durationby the median value of the REAL duration - SIMULATED DURATION
        total_error += trace_duration_error * np.abs(real_median_trace_duration-simulated_median_trace_duration) # weight the error on trace duration by the median value of the REAL duration - SIMULATED duration
        # normalize the total error (total error/(number of task*2+2) --> I multiply by two because the maximum difference in area between the two distributions is 2)
        # and I add 2 because is the maximum error given by the difference in area of the trace duration's distributions                
        #total_error = total_error/(len(task)*2+2) 

        # after having introduced the weight of each error with its median value, the difference in area is no more normalized to 2, but it can be whatever value, so
        # I can no more normalize the error
        print('The total error is: {}'.format(total_error))
        return total_error 


    def getTraceDistribution(self, log): 

        # compute the trace distribution of the log

        # compute the duration of each trace
        duration = self.getTraceDuration(log)
        # compute the distribution of the traces duration
        distribution = FitDistribution(pd.DataFrame(duration).astype('timedelta64[s]')).find_parameter()
        if distribution[0]!='Constant':
            if distribution[2] == False: ### points={"probability: [], "value: []}
                points, data = FitDistribution(pd.DataFrame(duration).astype('timedelta64[s]')).user_distribution(1)
                distribution = ('UserDistribution', points, data)
        median_trace_duration = np.median(duration).total_seconds()
        return distribution, median_trace_duration


    def getTraceDuration(self, log): #OK

        # compute the trace duration

        duration=[]
        for case_index in range(0, len(log)):
            first=log[case_index][0]['time:timestamp']
            last=log[case_index][-1]['time:timestamp']
            duration.append(last-first)
        return sorted(duration)

    #ERRORE CLAUDIA
    def getTraceDurationDistance(self, simulated_log):
        trace_duration_log=self.getTraceDuration(self.log)
        trace_duration_simulated_log=self.getTraceDuration(simulated_log)
        diff=0
        for i in range(len(trace_duration_log)):
            diff+=((trace_duration_log[i].total_seconds()-trace_duration_simulated_log[i].total_seconds()))**2
        diff=np.sqrt(diff/len(trace_duration_log))
        return diff

    #ERRORE CLAUDIA
    def getWaitingTimeDistance(self, simulated_log, task):
        print('Get wait real of:', task)
        wait_log=self.getWait(self.log, task)
        print('Get wait simulated of:', task)
        wait_simulated_log=self.getWait(simulated_log,task)
        diff=0
        #len_min=min(len(wait_log),len(wait_simulated_log))
        #for i in range(0,len_min):
        for i in range(len(wait_log)):
            if i<len(wait_simulated_log):
                diff+=((wait_log[i].total_seconds()-wait_simulated_log[i].total_seconds()))**2
            else:
                diff+=((wait_log[i].total_seconds()))**2
        diff=np.sqrt(diff/len(wait_log))
        return diff

    #ERRORE CLAUDIA
    def getWait(self, log, task):
        wait=[]
        for case_index, case in enumerate(log):
            for event_id,event in enumerate(case):
                idx = 1
                if log[case_index][event_id]['concept:name']==task:
                    while log[case_index][event_id-idx]['concept:name'] in self.parallel_activities:
                        idx += 1
                    wait.append(event['start:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None))
        return wait



    def computeRiemannDifference(self, real_distribution, simulated_distribution): 
        
        # compute the Riemann difference between the two distributions 

        pos = 0
        diff_area = 0
        points_real = []
        points_sim = []
        pos_x = []

        # compute the maximum between the two distributions
        maximum, max_real, max_sim, mode_real, mode_sim, quantile_real, quantile_sim = self.computeMaximum(real_distribution, simulated_distribution)
        

        # define the step with which we sample from the distributions
        step_sim = max_sim/1000 # step to use when I'm in a point where real_distribution is = 0, so that I sample with a step size that is in accordance with
                                # the dimension of the simulated_distribution
        step_real = max_real/1000 # step to use when I'm in a point where simulated_distribution is = 0, so that I sample with a step size that is in accordance with
                                  # the dimension of the real_distribution
        step = min(max_real, max_sim)/1000 # step for both distributions: while I'm sampling from both distributions (i.e. the distributions are both >0)
                                             # I use the minimum step between the two distributions, so that I sample many points where the two distributions
                                             # are both present

        # boundary cases
        if max_real==0 and max_sim==0:
            #print('The difference in area is: 0')
            return 0
        elif mode_real!=0 and max_sim==0:
            #print('The difference in area is: 2')
            return 2
        elif  max_real==0 and mode_sim!=0:
            #print('The difference in area is: 2')
            return 2
        elif mode_real == 0 and mode_sim == 0:
            if (max_real == 0 and max_sim != 0) or (max_real != 0 and max_sim == 0):
                if quantile_real == 0 and quantile_sim == 0:
                    return 0
                elif (quantile_real!=0 and quantile_sim == 0) or (quantile_real == 0 and quantile_sim != 0):
                    return 2

        while pos <= maximum:
            pos_x.append(pos)
            # REAL LOG
            # extract points only if pos <= max_real: indeed if pos>max_real it means that the funcion is zero in that point, so I avoid to extract the value
            # and I suddenly consider it 0
            if pos <= max_real:
                #print('SONO NEL CALCOLO DEL REAL')
                # Constant
                if real_distribution[0] == 'Constant':
                    #point_real = st.uniform.pdf(pos, loc = real_distribution[1], scale = 0)
                    point_real = st.uniform.pdf(pos, loc = real_distribution[2], scale = real_distribution[3])
                else:
                    # NegativeExponentialDistribution
                    if real_distribution[0] == 'NegativeExponentialDistribution':
                        point_real = st.expon.pdf(pos, loc = real_distribution[1][0], scale = real_distribution[1][1])
                    # UniformDistribution
                    if real_distribution[0] == 'UniformDistribution':
                        point_real = st.uniform.pdf(pos, loc = real_distribution[1][0], scale = real_distribution[1][1])
                    # TruncatedNormalDistribution
                    if real_distribution[0] == 'TruncatedNormalDistribution':
                        point_real = st.truncnorm.pdf(pos, a = real_distribution[1][2], b = real_distribution[1][3], loc = real_distribution[1][0], scale = real_distribution[1][1])
                    # TriangularDistribution
                    if real_distribution[0] == 'TriangularDistribution':
                        point_real = st.triang.pdf(pos, c = real_distribution[1][0], loc = real_distribution[1][1], scale = real_distribution[1][2])
                    # UserDistribution
                    if real_distribution[0] == 'UserDistribution': # points={"probability": [], "value": []}
                        data = np.array(real_distribution[2])
                        #print('INTERVAL', pos, pos+step)
                        point_real = len(data[np.logical_and(data>=pos, data<pos+step)])/len(data)
                        #print('POINT REAL', point_real)
                        points_real.append(points_real)
                        area_real = point_real
            else:
                point_real = 0
                if real_distribution[0] == 'UserDistribution':
                    points_real.append(point_real)
                    area_real = 0


            # SIMULATED LOG
            # extract points only if pos <= max_sim: indeed if pos>max_sim it means that the funcion is zero in that point, so I avoid to extract the value
            # and I suddenly consider it 0
            if pos <= max_sim:
                #print('SONO NEL CALCOLO DEL SIMULATED')
                # Constant
                if simulated_distribution[0] == 'Constant':
                    #point_sim = st.uniform.pdf(pos, loc = simulated_distribution[1], scale = 0)
                    point_sim = st.uniform.pdf(pos, loc = simulated_distribution[2], scale = simulated_distribution[3])
                else:
                    # NegativeExponentialDistribution
                    if simulated_distribution[0] == 'NegativeExponentialDistribution':
                        point_sim = st.expon.pdf(pos, loc = simulated_distribution[1][0], scale = simulated_distribution[1][1])
                        #print('PUNTO ESTRATTO SIMULATED', point_sim)
                    # UniformDistribution
                    if simulated_distribution[0] == 'UniformDistribution':
                        point_sim = st.uniform.pdf(pos, loc = simulated_distribution[1][0], scale = simulated_distribution[1][1])
                    # TruncatedNormalDistribution
                    if simulated_distribution[0] == 'TruncatedNormalDistribution':
                        point_sim = st.truncnorm.pdf(pos, a = simulated_distribution[1][2], b = simulated_distribution[1][3], loc = simulated_distribution[1][0], scale = simulated_distribution[1][1])
                    # TriangularDistribution
                    if simulated_distribution[0] == 'TriangularDistribution':
                        point_sim = st.triang.pdf(pos, c = simulated_distribution[1][0], loc = simulated_distribution[1][1], scale = simulated_distribution[1][2])
                    # UserDistribution
                    if simulated_distribution[0] == 'UserDistribution':
                        data = np.array(simulated_distribution[2])
                        #print('INTERVAL', pos, pos+step)
                        point_sim = len(data[np.logical_and(data>=pos, data<pos+step)])/len(data)
                        points_sim.append(point_sim)
                        area_sim = point_sim
            else:
                point_sim = 0
                if simulated_distribution[0] == 'UserDistribution':
                    points_sim.append(point_sim)
                    area_sim = 0

            

            if real_distribution[0] != 'UserDistribution':
                points_real.append(point_real)
                # caso in cui il singolo step è maggiore del massimo della distribuzione
                if step>max_real:
                    # calcolo l'area sotto la distribuzione approssimando la curva con un
                    # rettangolo con altezza=point_real e base=max_real (e non a step, perché
                    # è maggiore di max_real e mi darebbe un'area ≠1)
                    area_real = point_real*max_real
                else:
                    # caso in cui il singolo step NON è maggiore del massimo della distribuzione
                    if pos+step>max_real:
                        # caso in cui faccio sono in una certa posizione sull'asse x e mi muovo 
                        # di step, ma con questo step vado in un punto più largo di max_real
                        # (massimo della distribuzione): in questo caso considero come base
                        # del rettangolo con cui approssimo la distribuzione max_real-pos (che è 
                        # più piccolo di step)
                        area_real = point_real*(max_real-pos)
                    else:
                        # caso in cui step non è maggiore di max_real, cioè con il passo non sforo
                        # il massimo della distribuzione
                        area_real = point_real*step
            if simulated_distribution[0] != 'UserDistribution':
                points_sim.append(point_sim)
                # caso in cui il singolo step è maggiore del massimo della distribuzione
                if step>max_sim:
                    # calcolo l'area sotto la distribuzione approssimando la curva con un
                    # rettangolo con altezza=point_real e base=max_real (e non a step, perché
                    # è maggiore di max_real e mi darebbe un'area ≠1)
                    area_sim = point_sim*max_sim
                else:
                    # caso in cui il singolo step NON è maggiore del massimo della distribuzione
                    if pos+step>max_sim:
                        # caso in cui faccio sono in una certa posizione sull'asse x e mi muovo 
                        # di step, ma con questo step vado in un punto più largo di max_real
                        # (massimo della distribuzione): in questo caso considero come base
                        # del rettangolo con cui approssimo la distribuzione max_real-pos (che è 
                        # più piccolo di step)
                        area_sim = point_sim*(max_sim-pos)
                    else:
                        # caso in cui step non è maggiore di max_real, cioè con il passo non sforo
                        # il massimo della distribuzione
                        area_sim = point_sim*step
            
            
            diff_area += np.abs(area_real-area_sim)
            # if pos+step>=max_real, it means that pos+step is out from the maximum of the real_distribution. So from that point, I start 
            # sampling with a different step (step_sim) that will be larger than the previous step
            if pos+step>=max_real:
                step = step_sim
                pos += step_sim
            # if pos+step>=max_sim, it means that pos+step is out from the maximum of the simulated_distribution. So from that point, I start 
            # sampling with a different step (step_real) that will be larger than the previous step
            elif pos+step>=max_sim:
                step = step_real
                pos += step_real
            else:
                pos += step
            
        #plt.plot(pos_x, points_real, label='Real Distribution')
        #plt.legend()
        #plt.plot(pos_x, points_sim, label='Simulated Distribution')
        #plt.legend()
        #plt.show()

        print('The difference in area is: {}'.format(diff_area))
        return diff_area


    def computeMaximum(self, real_distribution, simulated_distribution): 

        # compute the maximum point of the real and simulated distributions: in input it takes the parameters that define the two distributions
        # and it will sample for 1000 times 1000 points for each distribution (real and simulated). From these 1000 points it takes the two maximum
        # points (of the real and simulated case) and then takes as maximum the maximum of these two extracted points. 
        # It repeats this process 1000 times and in the end it will take the maximum of the 1000 extracted maximums.
         
        max_real = []
        max_sim = []
        
        # consider 1000 random distributions of the same type of real_distribution and simulated_distribution and compute the avg maximum
        for i in range(1000):
            # REAL LOG 
            # Constant
            if real_distribution[0] == 'Constant':
                RD = st.uniform.rvs(loc = real_distribution[2], scale = real_distribution[3], size = 1000)
            else:
                # NegativeExponentialDistribution
                if real_distribution[0] == 'NegativeExponentialDistribution':
                    RD = st.expon.rvs(loc = real_distribution[1][0], scale = real_distribution[1][1], size = 1000)
                # UniformDistribution
                if real_distribution[0] == 'UniformDistribution':
                    RD = st.uniform.rvs(loc = real_distribution[1][0], scale = real_distribution[1][1], size = 1000)
                # TruncatedNormalDistribution
                if real_distribution[0] == 'TruncatedNormalDistribution':
                    RD = st.truncnorm.rvs(a = real_distribution[1][2], b = real_distribution[1][3], loc = real_distribution[1][0], scale = real_distribution[1][1], size = 1000)
                # TriangularDistribution
                if real_distribution[0] == 'TriangularDistribution':
                    RD = st.triang.rvs(c = real_distribution[1][0], loc = real_distribution[1][1], scale = real_distribution[1][2], size = 1000)
                # UserDistribution
                if real_distribution[0] == 'UserDistribution': ### points={"probability": [], "value": []}
                    RD = real_distribution[2].iloc[:,0] # real_distribution[2] = data --> then I take the maximum of the data
                    
            # SIMULATED LOG
            # Constant
            if simulated_distribution[0] == 'Constant':
                #SD = st.uniform.rvs(loc = simulated_distribution[1], scale = 0, size = 1000)
                SD = st.uniform.rvs(loc = simulated_distribution[2], scale = simulated_distribution[3], size = 1000)
            else:
                # NegativeExponentialDistribution
                if simulated_distribution[0] == 'NegativeExponentialDistribution':
                    SD = st.expon.rvs(loc = simulated_distribution[1][0], scale = simulated_distribution[1][1], size = 1000)
                # UniformDistribution
                if simulated_distribution[0] == 'UniformDistribution':
                    SD = st.uniform.rvs(loc = simulated_distribution[1][0], scale = simulated_distribution[1][1], size = 1000)
                # TruncatedNormalDistribution
                if simulated_distribution[0] == 'TruncatedNormalDistribution':
                    SD = st.truncnorm.rvs(a = simulated_distribution[1][2], b = simulated_distribution[1][3], loc = simulated_distribution[1][0], scale = simulated_distribution[1][1], size = 1000)
                # TriangularDistribution
                if simulated_distribution[0] == 'TriangularDistribution':
                    SD = st.triang.rvs(c = simulated_distribution[1][0], loc = simulated_distribution[1][1], scale = simulated_distribution[1][2], size = 1000)
                # UserDistribution
                if simulated_distribution[0] == 'UserDistribution':
                    SD = simulated_distribution[2].iloc[:,0]  # simulated_distribution[2] = data --> then I take the maximum of the data
                    
            max_real.append(max(RD))
            max_sim.append(max(SD))

        max_real = np.mean(max_real)
        max_sim = np.mean(max_sim)
        maximum = max(max_real, max_sim)

        # boundary cases
        try:
            mode_real= mode(RD)
        except:
            mode_real= 2 ### passo numero diverso da zero
        try: 
            mode_sim= mode(SD)
        except:
            mode_sim=2  ### passo numero diverso da zero

        quantile_real = np.quantile(RD, 0.85)
        quantile_sim = np.quantile(SD, 0.85)

        return maximum, max_real, max_sim, mode_real, mode_sim, quantile_real, quantile_sim


    def getEndToEnd(self, log, task): 

        # compute all the end-to-end duration of the task and fill the list to return

        endtoend = []

        for case_index, case in enumerate(log):
            for event_id, event in enumerate(case):
                if event['concept:name'] == task:
                    # take the index of the activity before the considered task
                    idx = 1
                    # if the task is performed in parallel with other activities, go back of another index to get the previous task
                    while log[case_index][event_id-idx]['concept:name'] in self.parallel_activities:
                        idx = idx + 1
                    if (event['time:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None)).total_seconds() >= 0:
                        endtoend.append(event['time:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None))
        return endtoend
    
    
    def getEndToEndDistribution(self, log): 
        
        # for each task in the log estimate the distribution of the end-to-end duration. If it is not possible to find a distribution
        # consider a User Distribution.

        endtoend_duration = {}
        distribution = {}
        
        for task in self.tasks:
            if task not in self.start_activities:
                print('****COMPUTING END TO END DISTRIBUTION FOR TASK {}****'.format(task))
                # compute the duration of each end-to-end time of the task 
                endtoend_duration[task] = self.getEndToEnd(log, task)
                # compute the distribution of the end-to-end duration 
                distribution[task] = FitDistribution(pd.DataFrame(endtoend_duration[task]).astype('timedelta64[s]')).find_parameter()
                if distribution[task][0] != 'Constant':
                    # compute the User Distribution if it is not possible to estimate e distribution 
                    if distribution[task][2] == False: # points={"probability: [], "value: []}
                        points, data = FitDistribution(pd.DataFrame(endtoend_duration[task]).astype('timedelta64[s]')).user_distribution(1)
                        distribution[task] = ('UserDistribution', points, data)
        return distribution



    def getWaiting(self, log, task):

        # compute all the waiting duration of the task and fill the list to return

        wait = []
        for case_index, case in enumerate(log):
            for event_id, event in enumerate(case):
                if log[case_index][event_id]['concept:name'] == task:
                    # take the index of the activity before the considered task
                    idx = 1
                    # if the task is performed in parallel with other activities, go back of another index to get the previous task
                    while log[case_index][event_id-idx]['concept:name'] in self.parallel_activities:
                        idx = idx + 1 
                    #print('EVENTO PRIMA TIMESTAMP',log[case_index][event_id-idx]['time:timestamp']) 
                    #print('EVENTO ATTUALE START TIMESTAMP', event['start:timestamp'])
                    #print('EVENTO PRIMA', log[case_index][event_id-idx]['concept:name'])
                    #print('EVENTO ATTUALE', event['concept:name'])
                    #if (event['start:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None)).total_seconds() >= 0:
                    #    print((event['start:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None)).total_seconds())
                    if (event['start:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None)).total_seconds() >= 0:
                        wait.append(event['start:timestamp'].replace(tzinfo=None) - log[case_index][event_id-idx]['time:timestamp'].replace(tzinfo=None))
        wait = sorted(wait)
        return wait


    def getWaitingDistribution(self, log): 

        # for each task in the log estimate the distribution of the waiting duration. If it is not possible to find a distribution
        # consider a User Distribution.

        waiting_duration = {}
        distribution = {}
        median_waiting_duration = {}

        # for each task:
        for task in self.tasks:
            if task not in self.start_activities:
                print('****COMPUTING WAITING DISTRIBUTION FOR TASK {}****'.format(task))
                # compute the duration of each waiting time of the task 
                waiting_duration[task] = self.getWaiting(log, task)
                median_waiting_duration[task] = np.median(waiting_duration[task]).total_seconds()
                # compute the distribution of the waiting duration 
                distribution[task] = FitDistribution(pd.DataFrame(waiting_duration[task]).astype('timedelta64[s]')).find_parameter()
                if distribution[task][0] != 'Constant':
                    if distribution[task][2] == False: ### points={"probability: [], "value: []}
                        points, data = FitDistribution(pd.DataFrame(waiting_duration[task]).astype('timedelta64[s]')).user_distribution(1)
                        distribution[task] = ('UserDistribution', points, data)
        return distribution, median_waiting_duration


    def getTaskBPMN(self): ### trovo tutti i task all'interno del BPMN
        file = open(self.path_bpmn, "r")
        tree = etree.parse(file)
        root = tree.getroot()
        self.tasks=[]

        process= root.find(BPMN2+"process")
        for child in process:
            if child.tag==BPMN2 +"task":
                self.tasks.append(child.get("name"))
                

    def getTaskLog(self): ### trovo tutti i task all'interno del log
        self.tasks=set()
        for trace in self.log:
            for event in trace:
                self.tasks.add(event['concept:name'])
    

    # Real Log (without start:timestamp) and Simulated Log
    def sim_alpha(self, errore):     
        # add the time:timestamp to the real log so that I can create the simulated log
        alpha_log=self.addStartTimestampAlphaRisorse_2(self.log, self.alpha)
        # add the time:timestamp to the real log so that I can create the simulated log
        #alpha_log=self.addEndTimestampAlphaRisorse_2(self.log, self.alpha) 
        # call Class SimulationBPSim
        if self.log_name == 'Colombia':
            bpsim=SimulationBPSIM_Colombia(alpha_log, self.path_bpmn)
        elif self.log_name == 'Purchasing':
            bpsim=SimulationBPSIM_Purchasing(alpha_log, self.path_bpmn)
        # simulation and reading file .log
        simulated_log= bpsim.run_simulation() 
        # sort the simulated log by 'time:timestamp'
        simulated_log = sorting.sort_timestamp(simulated_log, timestamp_key='time:timestamp')
        # sort the real log by 'time:timestamp'
        self.log = sorting.sort_timestamp(self.log, timestamp_key='time:timestamp')

        
        # COMPUTE THE ERROR BETWEEN THE REAL LOG AND THE SIMULATED LOG

        # if errore = 'end_end' I compute the error between the real log (without start:timestamps) and the simulated log (with certain alpha) 
        if errore=='end_end':
            # compute the error between the real log and the simulated log
            errore_sim = self.errore_endtoend(simulated_log)
        elif errore=='waiting_trace':
            # compute the error between the real log (with start:timestamp added with alpha) and the simulated log
            errore_sim = self.errore_waiting_trace(simulated_log)
        elif errore=='claudia':
            errore_sim = self.errore_claudia(simulated_log)
        return errore_sim