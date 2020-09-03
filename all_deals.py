from pipedrive import Pipedrive
import conf as conf
import sys
import pandas as pd

pipedrive = Pipedrive(conf.API_KEY)
df        = pd.DataFrame(columns=('add_time', 'lead_id', 'person_name', 'org_name', 'origem', 'person_id_phone', 'person_id_mail', 'owner_name', 'status',  'stage_change_time' ,  'lost_reason' ,'pipeline_id', 'won_time', 'activities_count', 'email_messages_count', 'stage_id', 'formatted_value', 'close_time'))

initial_status = 0

final_lines = []
for st in range(0,200, conf.LIMIT):    
    # calls the API
    results = pipedrive.deals({'start': st, 'limit': 500}, method='GET')
    # if there are results, go ahead!
    if results['data'] != None:       
        for r in results['data']:            
            if r['person_id'] != None and type(r['person_id']) != int:
                phone = r['person_id']['phone'][0]['value']
                email = r['person_id']['email'][0]['value']
            else :
                phone = ''
                email = ''
            
            df = df.append([{
                'add_time': r['add_time'], 
                'lead_id': r['id'], 
                'person_name': r['person_name'], 
                'org_name': r['org_name'], 
                'origem': '', 
                'person_id_phone': phone, 
                'person_id_mail': email, 
                'owner_name': r['owner_name'], 
                'status': r['status'], 
                'stage_change_time' : r['stage_change_time'], 
                'lost_reason' : r['lost_reason'], 
                'pipeline_id': r['pipeline_id'] ,
                'won_time': r['won_time'] ,
                'activities_count': r['activities_count'] , 
                'email_messages_count': r['email_messages_count'] ,
                'stage_id': r['stage_id'] ,
                'formatted_value': r['formatted_value'] ,
                'close_time': r['close_time'] ,
            }], ignore_index=True)            
    else:
        # If there are no more results, breaks the loop
        break
    
    print('Status=', st, 'tamanho do df ', len(df), 'ultimo lead_id', df['lead_id'].max())

print(df.head(10))

df.to_csv('all_deals.csv', index=False)

a = input('Pressione qualquer tecla para concluir o processamento...')
