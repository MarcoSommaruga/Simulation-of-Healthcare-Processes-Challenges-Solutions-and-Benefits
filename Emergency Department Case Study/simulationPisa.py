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

class SimulationBPSIM():
    
    def __init__(self, log, path_bpmn):
        
        self.path_bpmn=path_bpmn
        self.log=log
        self.roles = [['Letto', 12, 0], ['Medico turno 1', 4, 1],
                     ['Medico turno 2', 4, 2], ['Medico turno 3', 1, 3], 
                     ['Medico turno 4a', 2, 4], ['Medico turno 4b', 2, 5],
                     ['Infermiere turno 1', 5, 6], ['Infermiere turno 2', 5, 7], ['Infermiere turno 3a', 4, 8],
                     ['Infermiere turno 3b', 4, 9], ['Infermiere triage ambulanza turno 1', 1, 10], ['Infermiere triage autonomo turno 1', 1, 11], 
                     ['Infermiere triage ambulanza turno 2', 1, 12], ['Infermiere triage autonomo turno 2', 1, 13],
                     ['Infermiere triage turno 3a', 1, 14], ['Infermiere triage turno 3b', 1, 15], 
                     ['Tecnico Radiologia turno 1', 2, 16], ['Tecnico Radiologia turno 2', 2, 17], 
                     ['Tecnico Radiologia turno 3a', 2, 18], ['Tecnico Radiologia turno 3b', 2, 19], ['Radiologo turno 1', 1, 20],
                     ['Radiologo turno 2', 1, 21], ['Radiologo turno 3a', 1, 22], ['Radiologo turno 3b', 1, 23]]
        self.tasks = ['TRIAGE AMBULANZA',
                      'TRIAGE AUTONOMO',
                      'PRELIEVO',
                      'VISITA',
                      'LABORATORIO',
                      'RADIOLOGIA ESECUZIONE ECO',
                      'RADIOLOGIA REFERTAZIONE RX',
                      'RADIOLOGIA ESECUZIONE RX',
                      'RADIOLOGIA ESECUZIONE RMN',
                      'RADIOLOGIA REFERTAZIONE RMN',
                      'RADIOLOGIA ESECUZIONE Angio',
                      'RADIOLOGIA REFERTAZIONE Angio',
                      'RADIOLOGIA ESECUZIONE TAC',
                      'RADIOLOGIA REFERTAZIONE TAC',
                      'CONSULENZA',
                      'PRESTAZIONIPS',
                      'DIMISSIONE',
                      'OSSERVAZIONE']
        self.start_tasks=['TRIAGE AMBULANZA',
                          'TRIAGE AUTONOMO']
        self.calendars = [[('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['000000', '235959']], #0
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['080000', '140000']], #1
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['140000', '200000']], #2
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['200000', '235959']], #3
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['200000', '235959']], #4
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['000000', '080000']], #5
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['070000', '140000']], #6
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['140000', '220000']], #7
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['220000', '235959']], #8
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['000000', '070000']], #9
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['070000', '140000']], #10
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['070000', '140000']], #11
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['140000', '220000']], #12
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['140000', '220000']], #13
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['220000', '235959']], #14
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['000000', '070000']], #15
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['080000', '140000']], #16
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['140000', '200000']], #17
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['200000', '235959']], #18
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['000000', '080000']], #19
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['080000', '140000']], #20
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['140000', '200000']], #21
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['200000', '235959']], #22
                         [('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['000000', '080000']]] #23
        self.role_task = {'TRIAGE AMBULANZA':"orResource(getResource('Infermiere triage ambulanza turno 1', 1), getResource('Infermiere triage ambulanza turno 2', 1), getResource('Infermiere triage turno 3a', 1), getResource('Infermiere triage turno 3b', 1))",
                          'TRIAGE AUTONOMO':"orResource(getResource('Infermiere triage autonomo turno 1', 1), getResource('Infermiere triage autonomo turno 2', 1), getResource('Infermiere triage turno 3a', 1), getResource('Infermiere triage turno 3b', 1))",
                          'PRELIEVO':"orResource(getResource('Infermiere turno 1', 1), getResource('Infermiere turno 2', 1), getResource('Infermiere turno 3a', 1), getResource('Infermiere turno 3b', 1))",
                          'VISITA':"orResource(getResource('Medico turno 1', 1), getResource('Medico turno 2', 1), getResource('Medico turno 3', 1), getResource('Medico turno 4a', 1), getResource('Medico turno 4b', 1)) | orResource(getResource('Infermiere turno 1', 1), getResource('Infermiere turno 2', 1), getResource('Infermiere turno 3a', 1), getResource('Infermiere turno 3b', 1))",
                          'RADIOLOGIA ESECUZIONE ECO':"orResource(getResource('Radiologo turno 1', 1), getResource('Radiologo turno 2', 1), getResource('Radiologo turno 3a', 1), getResource('Radiologo turno 3b', 1))",
                          'RADIOLOGIA ESECUZIONE RX':"orResource(getResource('Tecnico Radiologia turno 1', 1), getResource('Tecnico Radiologia turno 2', 1), getResource('Tecnico Radiologia turno 3a', 1), getResource('Tecnico Radiologia turno 3b', 1))",
                          'RADIOLOGIA REFERTAZIONE RX':"orResource(getResource('Radiologo turno 1', 1), getResource('Radiologo turno 2', 1), getResource('Radiologo turno 3a', 1), getResource('Radiologo turno 3b', 1))",
                          'RADIOLOGIA ESECUZIONE RMN':"orResource(getResource('Tecnico Radiologia turno 1', 1), getResource('Tecnico Radiologia turno 2', 1), getResource('Tecnico Radiologia turno 3a', 1), getResource('Tecnico Radiologia turno 3b', 1))",
                          'RADIOLOGIA REFERTAZIONE RMN':"orResource(getResource('Radiologo turno 1', 1), getResource('Radiologo turno 2', 1), getResource('Radiologo turno 3a', 1), getResource('Radiologo turno 3b', 1))",
                          'RADIOLOGIA ESECUZIONE Angio':"orResource(getResource('Tecnico Radiologia turno 1', 1), getResource('Tecnico Radiologia turno 2', 1), getResource('Tecnico Radiologia turno 3a', 1), getResource('Tecnico Radiologia turno 3b', 1))",
                          'RADIOLOGIA REFERTAZIONE Angio':"orResource(getResource('Radiologo turno 1', 1), getResource('Radiologo turno 2', 1), getResource('Radiologo turno 3a', 1), getResource('Radiologo turno 3b', 1))",
                          'RADIOLOGIA ESECUZIONE TAC':"orResource(getResource('Tecnico Radiologia turno 1', 1), getResource('Tecnico Radiologia turno 2', 1), getResource('Tecnico Radiologia turno 3a', 1), getResource('Tecnico Radiologia turno 3b', 1))",
                          'RADIOLOGIA REFERTAZIONE TAC':"orResource(getResource('Radiologo turno 1', 1), getResource('Radiologo turno 2', 1), getResource('Radiologo turno 3a', 1), getResource('Radiologo turno 3b', 1))",
                          'PRESTAZIONIPS':"orResource(getResource('Medico turno 1', 1), getResource('Medico turno 2', 1), getResource('Medico turno 3', 1), getResource('Medico turno 4a', 1), getResource('Medico turno 4b', 1)) | orResource(getResource('Infermiere turno 1', 1), getResource('Infermiere turno 2', 1), getResource('Infermiere turno 3a', 1), getResource('Infermiere turno 3b', 1))",
                          'DIMISSIONE':"orResource(getResource('Medico turno 1', 1), getResource('Medico turno 2', 1), getResource('Medico turno 3', 1), getResource('Medico turno 4a', 1), getResource('Medico turno 4b', 1))",
                          'OSSERVAZIONE':"getResource('Letto', 1)"}

        self.probability = [('prelievo',0.115),('no_prelievo',0.885),
                           ('si attivita',0.857), ('no attivita',0.143),
                           ('osservazione',0.095), ('no_osservazione',0.905),
                           ('laboratorio',20636/(20636+16454)), ('no_laboratorio',16454/(20636+16454)),
                           ('loop laboratorio', 6782/(20636+16454)), ('out_laboratorio', (16454+20636-6782)/(20636+16454)),
                           ('consulenza',0.3166), ('angio',0.006), ('rx',0.213), ('eco',0.073), ('prestazionips',0.340), ('tac',0.060), ('rmn',0.0004), 
                           ('loop altre attivita',37794/(37794+30308)),('out_altre_attivita',30308/(37794+30308))]
        self.defineBPSim()
        
    def defineBPSim(self):
        self.BPSimpy = BPSimpy.BPSim(self.path_bpmn,verbosity=None)
        self.BPSimpy.addXmlns(name = 'lsim', value = "urn:lanner.simulation.lsim.model.data")
        self.SCENARIO= self.BPSimpy.addScenario(id = 'myscenario')
        # ADD SCENARIO PARAMETERS
        self.SCENARIO.addScenarioParameters(replication = 1, baseTimeUnit='s')
        self.SCENARIO.addStart(value=datetime.datetime(2017,1,1))
        self.SCENARIO.addDuration(value=datetime.timedelta(days=10000))
        
        # start-event
        # Ambulanza
        start=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName("Ambulanza"))
        start.addTriggerCount(value=10318)
        start.addInterTriggerTimer(nameDistribution='UserDistribution', discrete=True, points=pd.DataFrame(data={'probability': [0.358,0.360,0.137,0.064,0.029,0.018,0.012,0.007,0.004,0.003,0.002,0.002,0.001,0.001,0.002], 'value':[442.367496, 1353.294371, 2884.544813, 4375.843227, 5871.526846, 7395.308108, 8871.504132,10377.726027,11786.860465,13322.030303,14779.937500,16250.857143,17791.555556,19110.818182,20765.571429]}))
  

        # Autonomo
        start=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName("Autonomo"))
        start.addTriggerCount(value=25041)
        #start.addInterTriggerTimer(nameDistribution='UserDistribution', discrete = True, points=self.points_autonomo) 
        start.addInterTriggerTimer(nameDistribution='UserDistribution', discrete=True, points=pd.DataFrame(data={'probability': [0.853,0.110,0.021,0.007,0.004,0.002,0.001,0.001,0.001], 'value': [470.331196,1956.857247,4052.572779,6092.198864,8092.966292,10053.814815,12133.133333,13925.000000,15723.846154]}))

        #### add probability 
        for key in self.probability:
            flow=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName("%s"%(key[0]))) 
            flow.addProbability(value=key[1]) 
            
            
        ### add task processing time and Selection
        self.addProcessingTime()
        
        ### add quantity and calendar for role
        for role in self.roles:
            resource= self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName(role[0]))
            if role[0] == 'Letto':
                resource.addQuantity(value=role[1])
            else:
                resource.addQuantity(value = 0)
                resource.addQuantity(value=role[1], validFor=str(role[2]))
        for i,calendar in enumerate(self.calendars):
            cal1 = Calendar()
            cal1['begin']="VEVENT"
            cal1['dtstart'] = '20170101T' + calendar[1][0]
            cal1['dtend']= '20170101T' + calendar[1][1]
            cal1.add('rrule', {'freq': 'weekly', 'byday': calendar[0]})
            cal1['end']="VEVENT"
            cal1['version']="2.0"
            self.SCENARIO.addCalendar(name=str(self.roles[i][0]), id=str(str(i)), calendar=cal1)     
    
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
                activity=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName(task))
                if task=='TRIAGE AMBULANZA':
                    duration=timedelta(seconds = 120)
                    activity.addProcessingTime(duration)
                if task=='TRIAGE AUTONOMO': 
                    duration=timedelta(seconds = 180)
                    activity.addProcessingTime(duration)
            elif task=='LABORATORIO': 
                activity = self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName(task))
                activity.addProcessingTime(nameDistribution='UserDistribution', discrete = True, points=pd.DataFrame(data={'probability':[0.976,0.013,0.005,0.003,0.001,0.001,0.001],'value':[4608.581733,30303.788927,56022.132075,76988.375000,98188.476190,122210.454545,140327.090909]}))
            elif task=='OSSERVAZIONE':
                activity = self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName(task))
                activity.addProcessingTime(nameDistribution='NegativeExponentialDistribution', mean=58637.361935028246)
            elif task=='CONSULENZA':
                activity = self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName(task))
                activity.addProcessingTime(nameDistribution='NegativeExponentialDistribution', mean=3496.7041701073495)
            else:
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
            
    def addStartTimestamp(self):
        for trace in self.log:
                for idx, event in enumerate(trace):
                    if "start:timestamp" not in event:
                        if idx==0:
                            event["start:timestamp"]=event['time:timestamp']
                        else:
                            event["start:timestamp"]=trace[idx-1]['time:timestamp']
    
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
        input_file.readline() # skip first line
        log=lg.EventLog()
        traces=dict()
        for line in input_file:
            line=line.replace(",", "")  
            
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