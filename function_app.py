import json
import azure.functions as func
import logging

import pandas as pd
import numpy as np
import random
from datetime import datetime

# Purpose of this API is to create the schedule based on the preferences submitted

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="PowerApps")
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
		# Prepare necessary variables from JSON
        body = req.get_json()
        preferences = pd.DataFrame(body.get('Preferences'))
        bpp = pd.DataFrame(body.get('Preference Points'))
        users = pd.DataFrame(body.get('Users'))
        shift_timings = pd.DataFrame(body.get('Default Shift Timings'))
        shift_ids = [shift["Shift ID"] for shift in body.get('Shifts')]
		# dosow stands for date of start of week
        dosow = body.get('Start Date')

        # Main Code
    
        # Assuming 2 nurses are needed for each shift
        num_of_nurses_per_shift = 2

        #first filter the shifts table such thats its only relevant shift preferences
        p=preferences[preferences["Shift ID"].isin(shift_ids)]
        p=p[["User ID","Shift ID"]]
        distinct_users=p['User ID'].unique()


        #Lets calculate the popularity of each shift 
        temp_list=[]
        for shift_id in shift_ids:
            filtered_df=p[p["Shift ID"]==shift_id]
            temp_list.append(filtered_df.shape[0]/len(distinct_users))
        total_sum=sum(temp_list)
        #this is the probability of excluding the shift
        scaled_list=[x/total_sum for x in temp_list]
        adjusted_list=[1-x for x in scaled_list]
        del temp_list
        del total_sum
        del scaled_list
        #spd stands for shift popularity dictionary with the keys being the shift id and the value being the popularity factor
        spd=dict(zip(shift_ids,adjusted_list))
        del adjusted_list


        #Lets find the amount of preference points per user,sorted first by shift type then by tpp, utpp stands for user total preference points and each user is a distinct relevant user
        utpp={}
        bpp["Month"]=pd.to_datetime(bpp["Date"]).dt.month
        for user in distinct_users:#tpp is found for every distinct and relevant user
            fbpp=bpp[bpp["User ID"==user]&bpp['Point Change']<0&bpp["Month"]==datetime.strptime(dosow, "%Y-%m-%d").month]
            utpp[user]=10+fbpp['Point Change'].sum()*-1
        utpp=dict(sorted(utpp.items(),key = lambda item:item[1],reverse=True))
        temp=pd.DataFrame(utpp,columns=["User ID","TPP"])
        temp_users=users[["User ID","Shift Type"]]
        temp=temp.merge(temp_users,how="left",left_on="User ID",right_on="User ID")
        temp_1=temp[temp["Shift Type"]=="Regular"]
        temp_2=temp[temp["Shift Type"]=="Rotating"]
        temp_3=temp[temp["Shift Type"]=="Permanent Night"]
        uttp_regular=temp_1[["User ID","TPP"]].to_dict(orient="records")
        uttp_rotating=temp_2[["User ID","TPP"]].to_dict(orient="records")
        uttp_permanent_night=temp_3[["User ID","TPP"]].to_dict(orient="records")
        del temp
        del temp_users
        del temp_1
        del temp_2
        del temp_3

        #Lets find the type of working hours for each user 
        ust={}
        for user in distinct_users:
            shift_type=users[users["User ID"==user]]["Shift Type"]
            match shift_type:
                case"Regular":
                    ust[user]=42
                case "Rotating":
                    ust[user]=40
                case "Permanent Night":
                    ust[user]=38
            
            
        #Lets find a place to assign users to each shift
        shifts={}
        for shift in shift_ids:
            shifts[shift]=[]


        #Lets give a place to put duration per shift id
        shift_duration={}
        shift_index=1
        shift_timings["Duration"]=(pd.to_datetime(shift_timings['End Time'])-pd.to_datetime(shift_timings['Start Time'])).dt.total_seconds()/3600
        for shift in shift_ids:
            match shift_index:
                case 1:
                    shift_type="AM"
                case 2:
                    shift_type="PM"
                case 3:
                    shift_type="Night"
                    shift_duration[shift]=shift_timings[shift_timings["Type"]==shift_type]["Duration"]
            shift_index+=1
            if shift_index>3:
                shift_index =1
                
        #Lets give a place to put shift type per shift id
        shift_types={}
        shift_index=1
        shift_type=""
        for shift in shift_ids:
            match shift_index:
                case 1:
                    shift_type="AM"
                case 2:
                    shift_type="PM"
                case 3:
                    shift_type="Night"
            shift_types[shift]=shift_type
            shift_index+=1
            if shift_index>3:
                shift_index =1



        #there are 3 dictionaries where key is User ID and tpp is value, these 3 dictionaries are uttp_regular,uttp_rotating,uttp_permanent_night
        shift_completion={}       
        #Lets start allocating the shifts

        current_index=0
        failure=False

        number_of_permanent_night_nurses=len(uttp_permanent_night.keys())
        number_of_regular_nurses=len(uttp_regular.keys())
        number_of_rotating_nurses=len(uttp_rotating.keys())
        permanent_night_nurses=uttp_permanent_night.keys()
        regular_nurses=uttp_regular.keys()
        rotating_nurses=uttp_rotating.keys()

        #we first assign all the shifts for the permanent night nurses, then all the shifts for the regular nurses then fill in the remaining shifts with the rotating nurses 

        preference_failure=0
        current_index=0
        #Lets assign all the shifts for the permanent night shifts
        while not all(ust[key] <7.5 for key in uttp_permanent_night):
            failure=False
            #we get the User ID of the nurse
            selected_user=permanent_night_nurses[current_index]
            #we get the type of nurse we are allocating for 
            selected_user_shift_type=users[users["User ID"]==selected_user]["Shift Type"]
            #we select a random shift
            selected_shift = random.choices(list(spd.keys()), weights=spd.values(), k=1)
            #first condition is the shift is a night shift
            if shift_types[selected_shift]=="Night":
                #second condition is the shift must not be full and the nurse is not already assigned to the shift
                if len(shifts[selected_shift])<num_of_nurses_per_shift and selected_user not in shifts[selected_shift]:
                #third condition is the nurse must not be working excessively and doing the shift will not cross the max working time
                    if ust[selected_user]-shift_duration[selected_shift]>0:
                #fourth condition is check if shift is not in preference OR shifts has been rejected 3 times due to preference already
                        if selected_shift not in p[p["User ID"]==selected_user]["Shift ID"].to_list() or preference_failure==3:
                            shifts[selected_shift].append(selected_user)
                            if preference_failure==3:
                                preference_failure=0
                        else:#shift is in preference so failed due to preference
                            preference_failure+=1
                            failure=True
                    else:failure=True#if third condition is not met
                else:failure=True#if second condition is not met
            else:failure=True#if first condition is not met
            if failure==False:
                current_index+=1
                if current_index==number_of_permanent_night_nurses:#if this is true means has exceeded the index of the list 
                    current_index=0
                


        preference_failure=0
        current_index=0
        #Lets assign all the shifts for the regular shifts
        while not all(ust[key] <7.5 for key in uttp_regular):
            failure=False
            #we get the User ID of the nurse
            selected_user=regular_nurses[current_index]
            #we get the type of nurse we are allocating for 
            selected_user_shift_type=users[users["User ID"]==selected_user]["Shift Type"]
            #we select a random shift
            selected_shift = random.choices(list(spd.keys()), weights=spd.values(), k=1)
            #first condition is the shift is a night shift
            if shift_types[selected_shift]=="AM" or shift_types[selected_shift]=="PM":
            #second condition is the shift must not be full and the nurse is not already assigned to the shift
                if len(shifts[selected_shift])<num_of_nurses_per_shift and selected_user not in shifts[selected_shift]:
                #third condition is the nurse must not be working excessively and doing the shift will not cross the max working time
                    if ust[selected_user]-shift_duration[selected_shift]>0:
                #fourth condition is check if shift is not in preference OR shifts has been rejected 3 times due to preference already
                        if selected_shift not in p[p["User ID"]==selected_user]["Shift ID"].to_list() or preference_failure==3:
                            shifts[selected_shift].append(selected_user)
                            if preference_failure==3:
                                preference_failure=0
                        else:#shift is in preference so failed due to preference
                            preference_failure+=1
                            failure=True
                    else:failure=True#if third condition is not met
                else:failure=True#if second condition is not met
            else:failure=True#if first condition is not met
            if failure==False:
                current_index+=1
                if current_index==number_of_regular_nurses:#if this is true means has exceeded the index of the list 
                    current_index=0

        shifts_allocated=sum(len(lst) for lst in shifts.values())
        current_index=0
        while shifts_allocated<21*num_of_nurses_per_shift:
            failure=False
            #we get the User ID of the nurse
            selected_user=rotating_nurses[current_index]
            #we get the type of nurse we are allocating for 
            selected_user_shift_type=users[users["User ID"]==selected_user]["Shift Type"]
            #we select a random shift
            selected_shift = random.choices(list(spd.keys()), weights=spd.values(), k=1)
            #first condition is the shift is a night shift
            if shift_types[selected_shift]=="AM" or shift_types[selected_shift]=="PM" or shift_types[selected_shift]=="Night":
                #second condition is the shift must not be full and the nurse is not already assigned to the shift
                if len(shifts[selected_shift])<num_of_nurses_per_shift and selected_user not in shifts[selected_shift]:
                    #third condition is the nurse must not be working excessively and doing the shift will not cross the max working time
                    if ust[selected_user]-shift_duration[selected_shift]>0:
                        #fourth condition is check if shift is not in preference OR shifts has been rejected 3 times due to preference already
                        if selected_shift not in p[p["User ID"]==selected_user]["Shift ID"].to_list() or preference_failure==3:
                            shifts[selected_shift].append(selected_user)
                            if preference_failure==3:
                                preference_failure=0
                        else:#shift is in preference so failed due to preference
                            preference_failure+=1
                            failure=True
                    else:failure=True#if third condition is not met
                else:failure=True#if second condition is not met
            else:failure=True#if first condition is not met
            if failure==False:
                current_index+=1
                shifts_allocated+=1
                if current_index==number_of_rotating_nurses:#if this is true means has exceeded the index of the list 
                    current_index=0
                if all(ust[key] <7.5 for key in uttp_rotating):
                    break
        # Preparing data to be returned
        uncompleted_shifts=[{"Shift ID": shift} for shift in shifts.keys() if len(shifts[shift])<num_of_nurses_per_shift]
        shift_data = []
        for shift in shifts:
            for user in shifts[shift]:
                shift_data.append({"User ID": user,
                                   "Shift ID": shift})
        
        return_str = "hello" + name
        response_data = {"hello": return_str,
                         "User Shifts": shift_data,
                         "Unassigned Shifts": uncompleted_shifts}
        return func.HttpResponse(
            json.dumps(response_data),
            mimetype="application/json",
            status_code=200  
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )