##########
from icalendar import Calendar, Event
import pandas as pd
from datetime import datetime,timedelta
import datetime
import xml.etree.ElementTree as ET
from lxml import etree, objectify
import pytz
utc=pytz.UTC
import numpy as np
import os
import pm4py
from collections import defaultdict
import BPSimpy
import json
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.util import sorting
from FitDistribution import FitDistribution
from pm4py.objects.log import log as lg
import os

BPSIM='{http://www.bpsim.org/schemas/1.0}'
BPMN2='{http://www.omg.org/spec/BPMN/20100524/MODEL}'


#### Classe che prende BPMN e log, genera il BPSim e risponde con il log simulato

class SimulationBPSIM_Purchasing():
    
    def __init__(self, log,path_bpmn):
        
        self.path_bpmn=path_bpmn
        self.log=log
        self.roles = [['ROLE0',8], ['ROLE1',14], ['ROLE2',3], ['ROLE3',3], ['ROLE4',2], ['ROLE5',5], ['ROLE6',3], ['ROLE7',4]]
        self.tasks = ['Choose best option',                                             
                      'Settle Conditions With Supplier',              
                      'Create Purchase Order',                                        
                      'Send Invoice',                                                
                      'Confirm Purchase Order',                                        
                      'Deliver Goods Services',                                        
                      'Approve Purchase Order for payment',             
                      'Amend Request for Quotation',                   
                      'Authorize Supplier\'s Invoice payment',         
                      'Settle Dispute With Supplier',                             
                      'Release Purchase Order',                                        
                      'Analyze Purchase Requisition',               
                      'Send Request for Quotation to Supplier',          
                      'Pay Invoice',                                 
                      'Create Purchase Requisition',                                         
                      'Analyze Quotation Comparison Map',                     
                      'Analyze Request for Quotation',                           
                      'Release Supplier\'s Invoice',                    
                      'Create Request for Quotation',                         
                      'Create Quotation comparison Map']                     

        self.start_tasks=['Start']
        #self.start = datetime(2017,1,1)
        #self.duration = timedelta(days=600)

        #self.calendars = [[('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['20160201T000000', '20160628T230000']], #caseArrival    #0
        #                  [('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU'), ['20160201T000000', '20160201T230000']], #ROLE1Calendar  #1
        #                  [('TU', 'WE', 'FR', 'SU', 'TH', 'MO'), ['20160201T000000', '20160201T230000']], #ROLE3Calendar        #2
        #                  [('MO', 'WE', 'TH', 'FR', 'TU', 'SA'), ['20160201T000000', '20160201T230000']], #ROLE4Calendar        #3
        #                  [('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU'), ['20160201T000000', '20160201T230000']], #ROLE6Calendar  #4
        #                  [('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU'), ['20160201T000000', '20160201T230000']]] #ROLE10Calendar #5


        self.role_task = {'Choose best option':"getResource('ROLE1',1)",
                          'Settle Conditions With Supplier':"getResource('ROLE3',1)",
                          'Create Purchase Order':"getResource('ROLE3',1)",
                          'Send Invoice':"getResource('ROLE5',1)",
                          'Confirm Purchase Order':"getResource('ROLE5',1)",
                          'Deliver Goods Services':"getResource('ROLE5',1)",
                          'Approve Purchase Order for payment':"getResource('ROLE3',1)",
                          'Amend Request for Quotation':"getResource('ROLE1',1)",
                          'Authorize Supplier\'s Invoice payment':"getResource('ROLE4',1)",
                          'Settle Dispute With Supplier':"getResource('ROLE7',1)",
                          'Release Purchase Order':"getResource('ROLE1',1)",
                          'Analyze Purchase Requisition':"getResource('ROLE2',1)",
                          'Send Request for Quotation to Supplier':"getResource('ROLE3',1)",
                          'Pay Invoice':"getResource('ROLE4',1)",
                          'Create Purchase Requisition':"getResource('ROLE1',1)",
                          'Analyze Quotation Comparison Map':"getResource('ROLE1',1)",
                          'Analyze Request for Quotation':"getResource('ROLE3',1)",
                          'Release Supplier\'s Invoice':"getResource('ROLE4',1)",
                          'Create Request for Quotation':"getResource('ROLE6',1)",
                          'Create Quotation comparison Map':"getResource('ROLE3',1)"
                          }
        self.probability = [('sequenceFlows_8d5b0340-4277-d8e2-40ba-f086e808a1e6',0.61),('sequenceFlows_f1b58d91-7cea-3c71-2007-3e3efce0430f',0.39),
                            ('sequenceFlows_1eff4c1f-793a-1db7-28ca-6b5a168ce806',0.84),('sequenceFlows_886c0f8d-b84b-438e-7aad-a318a2a1e951',0.16),
                            ('sequenceFlows_a05575cb-8fa4-520c-0dfb-195793ccc7e7',0.45),('sequenceFlows_786de259-daf7-b91c-095a-b0dc17c43e6c',0.55),
                            ('sequenceFlows_465a2b59-784f-6a2a-b880-9a64866721f5',0.67),('sequenceFlows_a8147ba2-4afd-926b-bc4c-5616c5b75e7c',0.33),
                            ('sequenceFlows_e3a463f1-3254-1a3b-a99c-85bf83bcf560',0.21),('sequenceFlows_44cdab89-917e-d18e-d793-91d6f8325cc1',0.79)]                         
        
        self.defineBPSim()
        
    def defineBPSim(self):
        #### impostazione scenario
        self.BPSimpy = BPSimpy.BPSim(self.path_bpmn,verbosity=0)
        self.BPSimpy.addXmlns(name = 'lsim', value = "urn:lanner.simulation.lsim.model.data")
        self.SCENARIO= self.BPSimpy.addScenario(id = 'myscenario')
        ### ADD SCENARIO PARAMETERS
        self.SCENARIO.addScenarioParameters(replication = 1, baseTimeUnit='s')
        self.SCENARIO.addStart(value=datetime.datetime(2011,1,1))
        self.SCENARIO.addDuration(value=datetime.timedelta(days=10000))

        
        ### start-event
        start=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName("Start"))
        start.addTriggerCount(value=608) 
        start.addInterTriggerTimer(nameDistribution='NegativeExponentialDistribution', mean=19639.5)
        
        #### add probability 
        for elem in self.probability:
            flow=self.SCENARIO.getElementParameters(elem[0]) 
            flow.addProbability(value=elem[1])

            
        ### add task processing time and Selection
        self.addProcessingTime()
        
        
        ### add quantity for role
        for role in self.roles:
            print('ROLE', role)
            resource = self.SCENARIO.getElementParameters(role[0])
            resource.addQuantity(value=role[1])


    
    def getDuration(self, activity):
        duration=[]
        for trace in self.log:
            for event in trace:
                if event['concept:name']==activity:
                    time=event['time:timestamp'].replace(tzinfo=None) - event['start:timestamp'].replace(tzinfo=None)
                    duration.append(time)
        return duration
 

    def addProcessingTime(self):
        for task in self.tasks: 
            if task in self.start_tasks: 
                pass
            else:
                print('\n\nTASK:',task)
                duration=self.getDuration(task)
                distribution=FitDistribution(pd.DataFrame(duration).astype('timedelta64[s]')).find_parameter()
                activity=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName(task))
                if distribution[0]=='Constant':
                    points=FitDistribution(pd.DataFrame(duration).astype('timedelta64[s]')).user_distribution()
                    activity.addProcessingTime(nameDistribution='UserDistribution', discrete = True, points = points)
                else:
                    if distribution[2]==False: ### points={"probability: [], "value: []}
                        points=FitDistribution(pd.DataFrame(duration).astype('timedelta64[s]')).user_distribution()
                        activity.addProcessingTime(nameDistribution='UserDistribution', discrete = True, points = points)
                    else:
                        if distribution[0]=='NegativeExponentialDistribution':
                            activity.addProcessingTime(nameDistribution=distribution[0], mean=distribution[1][1])
                        elif distribution[0]=='UniformDistribution':
                            activity.addProcessingTime(nameDistribution=distribution[0], min=distribution[1][0], max=distribution[1][1])
                        elif distribution[0]=='TruncatedNormalDistribution':
                            activity.addProcessingTime(nameDistribution=distribution[0], mean=distribution[1][0], standardDeviation=distribution[1][1], min=distribution[1][2], max=distribution[1][3])
                        elif distribution[0]=='GammaDistribution':
                            activity.addProcessingTime(nameDistribution=distribution[0], shape=distribution[1][0], scale=distribution[1][1])
                        elif distribution[0]=='TriangularDistribution':
                            if distribution[1][1]<0:
                                distribution[1][1]=0
                            activity.addProcessingTIme(nameDistribution=distribution[0], mode=distribution[1][0], min=distribution[1][1], max=distribution[1][2])

            tree = etree.Element("TaskConfiguration")
            child = etree.SubElement(tree,"EventLog")
            activity.addVendorExtension(name = 'l-sim-task-configuration-v1', tree_list = [tree])

            if task in self.role_task:
                activity.addSelection(expression=self.role_task[task])
        
        
    def import_log(self, path_log):
        try:
            log= xes_importer.apply(path_log)
            log= sorting.sort_timestamp(log)
            if not "start:timestamp" in log[0][0]:
                if "org:resource" in log[0][0]:
                    self.addStartTimestampResource()
                else:
                    self.addStartTimestamp()
            return log
        except FileNotFoundError:
            print("ERROR: File not found")
            
    def addStartTimestamp(self): ## tecnica base complete-complete
        for trace in self.log:
                for idx, event in enumerate(trace):
                    if "start:timestamp" not in event:
                        if idx==0:
                            event["start:timestamp"]=event['time:timestamp']
                        else:
                            event["start:timestamp"]=trace[idx-1]['time:timestamp']
    
    #misura durata come end- max(end_precedente, ultima volta che risorsa è stata occupata)
    def addStartTimestampResource(self):
            dic=dict()
            for case_index, case in enumerate(self.log):
                for event_id,event in enumerate(case):
                    start=event["time:timestamp"]
                    if event_id>0:
                        if event['org:resource'] not in dic:
                            start=case[event_id-1]['time:timestamp']
                        else:
                            if dic[event['org:resource']]<event["time:timestamp"]:
                                start=max(case[event_id-1]['time:timestamp'],dic[event['org:resource']] )
                            else:
                                start=case[event_id-1]['time:timestamp']
                    event["start:timestamp"]=start
                    dic[event['org:resource']]=event['time:timestamp']
                    
    
    def run_simulation(self):
        os.system("java -jar l-sim-2.7.355.jar BPSIM_output.xml")
        simulated_log=self.from_text_to_log()  
        return simulated_log

    def from_text_to_log(self):
        input_file = open('myscenario_R000_events.log', 'r')
        input_file = open('/Users/marcosommaruga/Downloads/myscenario_R000_events_0 (3).log', 'r')
        input_file.readline() # skip first line
        log=lg.EventLog()
        traces=dict()
        for line in input_file:
            line=line.replace(",", "")  ### togli il punto
            
            list_value = line.strip().split('\t')
            if int(list_value[2]) not in traces:
                new_case=lg.Trace()
                new_case._set_attributes({'concept:name':int(list_value[2])})
                new_case.insert(int(list_value[2]),{'Scenario': list_value[0],
                                   'Replication': list_value[1],
                                   'case': list_value[2],
                                   'concept:name': list_value[3],
                                   'Arrived': datetime.datetime.strptime(list_value[4],"%Y-%m-%d %H:%M:%S").replace(tzinfo=utc),
                                   'start:timestamp': datetime.datetime.strptime(list_value[5],"%Y-%m-%d %H:%M:%S").replace(tzinfo=utc),
                                   'time:timestamp': datetime.datetime.strptime(list_value[6],"%Y-%m-%d %H:%M:%S").replace(tzinfo=utc),
                                   'org:resource': list_value[7:len(list_value)]})
                traces[int(list_value[2])]=new_case
            else:
                traces[int(list_value[2])].append({'Scenario': list_value[0],
                                   'Replication': list_value[1],
                                   'case': list_value[2],
                                   'concept:name': list_value[3],
                                   'Arrived': datetime.datetime.strptime(list_value[4],"%Y-%m-%d %H:%M:%S").replace(tzinfo=utc),
                                   'start:timestamp': datetime.datetime.strptime(list_value[5],"%Y-%m-%d %H:%M:%S").replace(tzinfo=utc),
                                   'time:timestamp': datetime.datetime.strptime(list_value[6],"%Y-%m-%d %H:%M:%S").replace(tzinfo=utc),
                                   'org:resource': list_value[7:len(list_value)]})

        for trace in traces:
            log.append(traces[trace])
        input_file.close()
        return log
    

    
