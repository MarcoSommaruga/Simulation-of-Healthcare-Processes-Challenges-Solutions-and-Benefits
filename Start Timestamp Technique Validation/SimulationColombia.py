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

class SimulationBPSIM_Colombia():
    
    def __init__(self, log,path_bpmn):
        
        self.path_bpmn=path_bpmn
        self.log=log

        self.roles = [['ROLE0'],['ROLE1', 8, 'ROLE1Calendar'],['ROLE2'],['ROLE3', 1, 'ROLE3Calendar'],['ROLE4', 5, 'ROLE4Calendar'],
                     ['ROLE5', 1],['ROLE6', 5, 'ROLE6Calendar'],['ROLE7'],['ROLE8'],['ROLE9'],
                     ['ROLE10', 5, 'ROLE10Calendar'],['ROLE11'],['ROLE12']]

        self.tasks = ['Evaluacion curso',
                      'Homologacion por grupo de cursos',
                      'Traer informacion estudiante - banner',
                      'Cancelar Solicitud',
                      'Notificacion estudiante cancelacion soli',
                      'Validar solicitud',
                      'Revisar curso',
                      'Radicar Solicitud Homologacion']

        self.start_tasks=['start']

        self.calendars = [[('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU'), ['20160201T000000', '20160628T230000']], #caseArrival    #0
                          [('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU'), ['20160201T000000', '20160201T230000']], #ROLE1Calendar  #1
                          [('TU', 'WE', 'FR', 'SU', 'TH', 'MO'), ['20160201T000000', '20160201T230000']], #ROLE3Calendar        #2
                          [('MO', 'WE', 'TH', 'FR', 'TU', 'SA'), ['20160201T000000', '20160201T230000']], #ROLE4Calendar        #3
                          [('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU'), ['20160201T000000', '20160201T230000']], #ROLE6Calendar  #4
                          [('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU'), ['20160201T000000', '20160201T230000']]] #ROLE10Calendar #5

        self.role_task = {'Evaluacion curso':"getResource('ROLE3',1)",
                          'Homologacion por grupo de cursos':"getResource('ROLE4',1)",
                          'Traer informacion estudiante - banner':"getResource('ROLE5',1)",
                          'Cancelar Solicitud':"getResource('ROLE1',1)",
                          'Notificacion estudiante cancelacion soli':"getResource('ROLE1', 1)",
                          'Validar solicitud':"getResource('ROLE10',1)",
                          'Revisar curso':"getResource('ROLE6',1)",
                          'Radicar Solicitud Homologacion':"getResource('ROLE5', 1)"}

        self.probability = [('idfe7a39dd-9ab9-479d-8f55-bb4d97df711c',0.540650406504065),('ida240f1b4-920e-4645-b11e-3770671a7c67',0.45934959349593496),
                            ('id4d3606b0-e331-43ff-ba40-273ac6d7587e',0.5691823899371069),('idd1a2e5e2-0fa6-4b35-9790-d2db483837e5',0.4308176100628931),
                            ('id335d64d5-419b-4bc0-9f86-8ad2c1265a3f',0.5589430894308943),('idec31294e-dfc0-498e-a0b4-b1e308c8c783',0.4410569105691057),
                            ('ida28d67be-b8a8-4e10-b6fa-0256a4e54aa5',0.48),('idae7b7f50-b1df-4db8-9cc9-9bcd511895a1',0.52)]
                         
        self.defineBPSim()
        
    def defineBPSim(self):
        #### impostazione scenario
        self.BPSimpy = BPSimpy.BPSim(self.path_bpmn,verbosity=0)
        self.BPSimpy.addXmlns(name = 'lsim', value = "urn:lanner.simulation.lsim.model.data")
        self.SCENARIO= self.BPSimpy.addScenario(id = 'myscenario')
        ### ADD SCENARIO PARAMETERS
        self.SCENARIO.addScenarioParameters(replication = 1, baseTimeUnit='s')
        self.SCENARIO.addStart(value=datetime.datetime(2016,2,1))
        self.SCENARIO.addDuration(value=datetime.timedelta(days=10000))
        self.SCENARIO.addWarmup(value=datetime.timedelta(days=1))
        
    
        ### start-event
        start=self.SCENARIO.getElementParameters(self.BPSimpy.getIdByName("start"))
        start.addTriggerCount(value=954) 
        start.addInterTriggerTimer(nameDistribution='NegativeExponentialDistribution', mean=13408.63168940189, validFor='caseArrival')
        
        #### add probability 
        for elem in self.probability:
            flow=self.SCENARIO.getElementParameters(elem[0]) 
            flow.addProbability(value=elem[1]) 
            
            
        ### add task processing time and Selection
        self.addProcessingTime()
        
        
        ### add quantity and calendar for role
        for role in self.roles:
            #print(role[0])
            if len(role)>1:
                if role[0]=='ROLE5':
                    resource = self.SCENARIO.getElementParameters(role[0])
                    resource.addQuantity(value=role[1])
                else:
                    resource= self.SCENARIO.getElementParameters(role[0])
                    resource.addQuantity(value = 0)
                    resource.addQuantity(value=role[1], validFor=str(role[2]))

        
        # create calendar: caseArrival
        cal1 = Calendar()
        cal1['begin']="VCALENDAR"
        cal1['dtstart'] = '20160201T000000'
        cal1['dtend']= '20160628T230000'
        cal1.add('rrule', {'freq': 'daily', 'byday': ('MO', 'TU', 'WE', 'TH', 'FR', 'SA', 'SU')})
        cal1['end']="VEVENT"
        cal1['version']="2.0"
        self.SCENARIO.addCalendar(name='caseArrival', id='caseArrival', calendar=cal1)

        # create calendar: ROLE1Calendar
        cal1 = Calendar()
        cal1['begin']="VCALENDAR"
        cal1['dtstart'] = '20160201T000000'
        cal1['dtend']= '20160201T230000'
        cal1.add('rrule', {'freq': 'daily', 'byday': ('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU')})
        cal1['end']="VEVENT"
        cal1['version']="2.0"
        self.SCENARIO.addCalendar(name='ROLE1Calendar', id='ROLE1Calendar', calendar=cal1)
        
        # create calendar: ROLE3Calendar
        cal1 = Calendar()
        cal1['begin']="VCALENDAR"
        cal1['dtstart'] = '20160201T000000'
        cal1['dtend']= '20160201T230000'
        cal1.add('rrule', {'freq': 'daily', 'byday': ('TU', 'WE', 'FR', 'SU', 'TH', 'MO')})
        cal1['end']="VEVENT"
        cal1['version']="2.0"
        self.SCENARIO.addCalendar(name='ROLE3Calendar', id='ROLE3Calendar', calendar=cal1)

        # create calendar: ROLE4Calendar
        cal1 = Calendar()
        cal1['begin']="VCALENDAR"
        cal1['dtstart'] = '20160201T000000'
        cal1['dtend']= '20160201T230000'
        cal1.add('rrule', {'freq': 'daily', 'byday': ('MO', 'WE', 'TH', 'FR', 'TU', 'SA')})
        cal1['end']="VEVENT"
        cal1['version']="2.0"
        self.SCENARIO.addCalendar(name='ROLE4Calendar', id='ROLE4Calendar', calendar=cal1)

        # create calendar: ROLE6Calendar
        cal1 = Calendar()
        cal1['begin']="VCALENDAR"
        cal1['dtstart'] = '20160201T000000'
        cal1['dtend']= '20160201T230000'
        cal1.add('rrule', {'freq': 'daily', 'byday': ('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU')})
        cal1['end']="VEVENT"
        cal1['version']="2.0"
        self.SCENARIO.addCalendar(name='ROLE6Calendar', id='ROLE6Calendar', calendar=cal1)

        # create calendar: ROLE10Calendar
        cal1 = Calendar()
        cal1['begin']="VCALENDAR"
        cal1['dtstart'] = '20160201T000000'
        cal1['dtend']= '20160201T230000'
        cal1.add('rrule', {'freq': 'daily', 'byday': ('WE', 'TU', 'FR', 'TH', 'MO', 'SA', 'SU')})
        cal1['end']="VEVENT"
        cal1['version']="2.0"
        self.SCENARIO.addCalendar(name='ROLE10Calendar', id='ROLE10Calendar', calendar=cal1)

    
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
        simulated_log=self.from_text_to_log()  ## ricordarsi di fare preprocessing ex. mettere labels giusti
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
    

    
