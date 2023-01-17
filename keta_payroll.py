import sqlalchemy
from sqlalchemy.orm import sessionmaker
from flask import Flask, request, jsonify, abort, Response
from datetime import datetime, timedelta
import json
import logging
import os


def handle(request):
    if request.method == 'POST':
        data = request.data.decode()
        action_type = request.args.get('type')
        if action_type == 'create-profile':
            keta_service = KetaCandidateService(candidate_info=json.loads(data))
            response = keta_service.execute()
        else:
            return abort(403)

        if response:
            response = jsonify(response)
        else:
            response = jsonify({"status_code": 201, "message": "Created new candidate successfully"})
            
        response.headers.add("Access-Control-Allow-Origin", os.getenv('origin'))
        return response

    elif request.method == 'GET':
        action_type = request.args.get('type')

        if not action_type:
            return abort(403)

        keta_service = KetaCandidateService(candidate_info=None)
        result = list()

        if action_type == 'get-positions':
            result = keta_service.get_all_positions()
        elif action_type == 'get-employment-status':
            result = keta_service.get_employment_status()
        elif action_type == 'get-roles':
            result = keta_service.get_all_roles()
        elif action_type == 'get-job-titles':
            result = keta_service.get_all_job_titles()
        elif action_type == 'get-departments':
            result = keta_service.get_all_departments()
        elif action_type == 'get-business-units':
            result = keta_service.get_all_business_unit()
        elif action_type == 'get-recruiters':
            result = keta_service.get_recruiters()
        elif action_type == 'get-vendors':
            result = keta_service.get_vendors()
        elif action_type == 'get-reporting-managers':
            result = keta_service.get_all_reporting_managers()
        elif action_type == 'get-employees':
            result = keta_service.get_all_employees()
        elif action_type == 'get-active-projects':
            result = keta_service.get_active_projects()
        elif action_type == 'get-timesheet-report':
            month = request.args.get('month')
            year = request.args.get('year')
            result = keta_service.get_timesheet_info(month, year)
        elif action_type  == 'get-employee-details':
            result = keta_service.get_employee_details()
        elif action_type  == 'get-task-details':
            result = keta_service.get_task_details()
        elif action_type  == 'get-bench-hours':
            result = keta_service.get_bench_hours()
        elif action_type == 'timesheet-defaulter':
            result = keta_service.timesheet_defaulters_list()
        elif action_type  == 'get-timesheet-defaulters':
            result = keta_service.get_timesheet_defaulters()
        elif action_type  == 'get-historic-timesheet-data':
            result = keta_service.get_historic_timesheet()
        elif action_type == 'get-utilization-details':
            result = keta_service.get_utilization_details()
        elif action_type == 'get-monthly-bench-report':
            result = keta_service.get_monthly_bench_report()
        else:
            return abort(403)

        response = jsonify({"data": result, "action_type": action_type})
        response.headers.add("Access-Control-Allow-Origin", os.getenv('origin'))
        return response
    else:
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", os.getenv('origin'))
        return response

class KetaCandidateService:

    def __init__(self, candidate_info):
        self.engine = None
        self.session = None
        self.candidate_info = candidate_info

    def execute(self):
        try:
            self.session = self.create_session()
            candidate_exist = self.check_user_exist()

            if isinstance(candidate_exist, dict) and candidate_exist.get('error_message'):
                self.close_database_connection()
                return candidate_exist

            if candidate_exist:
                self.close_database_connection()
                return {"status_code": 409, \
                    "message": f"The user with email {self.candidate_info['company_email_address']} already exist in the keta"}

            self.create_user()
        except Exception as error:
            logging.exception(error)
            self.close_database_connection()
            response = {"status_code": 500, \
                    "error_message": error}
            return error

    def create_user(self):
        id = self.get_recently_created_user_id_and_emp_id()
        first_name = self.candidate_info.get('emp_first_name')
        last_name = self.candidate_info.get('emp_last_name')
        user_id = id + 1
        data = {
            "id": user_id,
            "emprole": self.candidate_info['role_id'],
            "userstatus": "old",
            "firstname": first_name,
            "lastname": last_name,
            "userfullname": self.candidate_info['candidate_name'],
            "emailaddress": self.candidate_info['company_email_address'],
            "contactnumber": self.candidate_info.get('contact_number'),
            "empipaddress": None,
            "backgroundchk_status": "Yet to start",
            "emptemplock": 0,
            "empreasonlocked": None,
            "emplockeddate": None,
            "emppassword": None,
            "createdby": 1,
            "modifiedby": 1,
            "createddate": f"{datetime.now()}",
            "modifieddate": f"{datetime.now() + timedelta(minutes=6)}",
            "isactive": 1,
            "employeeId": self.candidate_info.get('emp_id'),
            "modeofentry": self.candidate_info.get('source_type'),
            "other_modeofentry": None,
            "entrycomments": None,
            "rccandidatename": None,
            "selecteddate": self.candidate_info.get('joining_date'),
            "candidatereferredby": None,
            "company_id": None,
            "profileimg": None,
            "jobtitle_id": self.candidate_info['job_title_id'],
            "tourflag": 1,
            "themes": "default"
        }

        try:
            metadata = sqlalchemy.MetaData(bind=self.engine)
            table = sqlalchemy.Table('main_users', metadata, autoload_with=self.engine)
            insert_statement = table.insert().values(**data)
            insert_statement.execute()
            self.create_employee(user_id)
        except Exception as err:
            logging.exception(err)
    
    def create_employee(self, user_id):
        emp_id = self.get_recently_created_employee_id() + 1
        data = {
            "jobtitle_id" : self.candidate_info.get('job_title_id'),
            "service_category" : None,
            "user_id" : user_id,
            "emp_status_id" : self.candidate_info.get('employment_status_id'), 
            "extension_number": None,
            "is_orghead" : 0,
            "createdby": 1, 
            "date_of_leaving": None,
            "reporting_manager": self.candidate_info.get('reporting_manager_id'),
            "id" : emp_id,
            "years_exp": self.candidate_info.get('years_of_experience'),
            "holiday_group": None,
            "modifieddate": f"{datetime.now()}",
            "modifiedby": 1,
            "track":  self.candidate_info.get('track'),
            "referral_agency": None,
            "levels":  self.candidate_info.get('levels'),
            "office_faxnumber": None,
            "office_number": None,
            "createddate": f"{datetime.now()}",
            "businessunit_id": self.candidate_info.get('business_unit_id'),
            "isactive": 1,
            "department_id": self.candidate_info.get('department_id'),
            "date_of_joining": self.candidate_info.get('joining_date'),
            "technology": self.candidate_info.get('technologies'),
            "category": self.candidate_info.get('category'),
            "exp_range": self.candidate_info.get('experience_range'),
            "prefix_id": self.candidate_info.get('emp_prefix_id'),
            "practice_areas": self.candidate_info.get('practice_areas'),
            "region": self.candidate_info.get('region')
        }
        try:
            metadata = sqlalchemy.MetaData(bind=self.engine)
            table = sqlalchemy.Table('main_employees', metadata, autoload_with=self.engine)
            insert_statement = table.insert().values(**data)
            insert_statement.execute()
        except Exception as err:
            logging.exception(err)

    def close_database_connection(self):
        if self.session:
            self.session.close()

        if self.engine:
            self.engine.dispose()

    def create_session(self):
        self.engine = self.create_engine()
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session

    def check_user_exist(self):
        email = self.candidate_info.get('company_email_address')
        if not email:
            return {"status_code": 400, "error_message": "'company_email_address' doesn't exist in the payload"}

        query = f"select emailaddress FROM sentri.main_users where emailaddress = '{email}'"
        result = self.get_executed_result(query)

        user_exist = False if len(result) == 0 else True
        return user_exist

    def get_recently_created_emp_id(self):
        query = "SELECT id FROM sentri.main_employees order by id desc limit 1"
        result = self.get_executed_result(query)
        return int(result[0]['id'])

    def get_recently_created_employee_id(self):
        query = "SELECT id FROM sentri.main_employees order by id desc limit 1"
        result = self.get_executed_result(query)
        return int(result[0]['id'])

    def get_recently_created_user_id_and_emp_id(self):
        query = "SELECT id, employeeId FROM sentri.main_users order by id desc limit 1"
        result = self.get_executed_result(query)
        return int(result[0]['id'])
    
    def get_all_positions(self):
        self.session = self.create_session()
        query = "select id, positionname  from sentri.main_positions where isactive=1"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_all_roles(self):
        self.session = self.create_session()
        query = "select id, rolename  from main_roles where isactive=1 and group_id is not NULL"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_recruiters(self):
        self.session = self.create_session()
        query = "select user_id, userfullname from main_employees_summary mes where department_name ='Recruitment' and isactive =1"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_vendors(self):
        self.session = self.create_session()
        query = "select * from main_rm_vendors"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_all_job_titles(self):
        self.session = self.create_session()
        query = "select id, jobtitlename  from main_jobtitles where isactive=1"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_timesheet_info(self, month, year):
        self.session = self.create_session()
        query = f"""SELECT distinct tet.emp_id as userid,mes.employeeId,mes.userfullname,mes.emailaddress,tet.ts_month
        ,sum(tet.week_duration) as user_time,tet.ts_year,tp.project_name, tp.project_type FROM sentri.tm_emp_timesheets tet
        inner join
        main_employees_summary mes on tet.emp_id=mes.user_id
        inner join tm_project_employees tpe on
        mes.user_id=tpe.emp_id
        inner join tm_projects tp
        on tpe.project_id=tp.id
        where tet.ts_month ={month} and tet.ts_year={year} and tpe.is_active=1
        and tp.is_active=1 group by
        tet.emp_id,tet.ts_month,tpe.emp_id"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_employment_status(self):
        self.session = self.create_session()
        query = "select id, description from main_employmentstatus me where isactive=1 and workcode not in ('RES')"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_all_reporting_managers(self):
        self.session = self.create_session()
        query = "SELECT distinct mes.reporting_manager_name, mes.reporting_manager as reporting_manager_id, me.emailaddress FROM sentri.main_employees_summary mes inner join main_users me on mes.reporting_manager=me.id where mes.isactive=1;" # Job title 2 is managers
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_all_employees(self):
        self.session = self.create_session()
        query = f"""SELECT mu.id, userfullname, firstname, lastname, date_of_joining,  emailaddress, employeeId, mu.createddate, emprole FROM sentri.main_users mu
                left join main_employees me on me.user_id = mu.id
                where mu.isactive=1 and mu.id  <> '1'"""
                
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_monthly_bench_report(self):
        self.session = self.create_session()
        query = f"""SELECT mes.employeeId, mes.userfullname, mes.reporting_manager_name, mes.primary_skill, tc.client_name, tp.project_name,
            IF(mes.businessunit_name in ('Bangalore','Chennai','Hyderabad'),'IND', 'USA')  as Country, mes.emp_status_name,
            tp.project_code, tp.start_date, tp.end_date, tp.po_number, tp.sow_number, tp.project_status,
            sum(if(tts.mon_status = 'Approved',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved',hour(tet.tue_duration),0))
            +sum(if(tts.wed_status = 'Approved',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved',hour(tet.thu_duration),0))
            +sum(if(tts.fri_status = 'Approved',hour(tet.fri_duration),0)) as 'Bench_Hours',
            MONTHNAME(STR_TO_DATE(month(DATE_SUB(now(), INTERVAL 1 month)), '%m')) as 'Month'
            from tm_emp_timesheets tet join tm_ts_status tts
            on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week
            right join main_employees_summary mes on tts.emp_id = mes.user_id
            join tm_projects tp on tts.project_id = tp.id
            join tm_clients tc on tp.client_id=tc.id where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month))
            and tts.ts_month = month(DATE_SUB(now(), INTERVAL 1 month)) and mes.isactive = 1 and tet.project_task_id in
            (select id from tm_project_tasks tpt where task_id in (444,452)) group by mes.employeeId order by mes.employeeId;"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_utilization_details(self):
        self.session = self.create_session()
        query = f"""SELECT query9.*, query10.Working_Days from (SELECT query5.*, query6.Bench_Hours from (select query3.*, query4.Leave_Hours from 
            (select query1.*, query2.Billed_Hours
            from (select query11.*, query12.* from (SELECT mes.employeeId,mes.userfullname, mes.emp_status_name, mes.reporting_manager_name, mes.date_of_joining, mes.date_of_leaving,
            mes.modeofentry, mes.years_exp, tc.client_name, tp.id, tp.project_name, tp.project_code, tp.po_number, tp.sow_number,  
            tp.start_date, tp.end_date, tp.project_status, tp.project_type, tpe.created,
            IF(mes.businessunit_name in ('Bangalore','Chennai','Hyderabad'),'IND', 'USA')  as country
            FROM sentri.tm_project_employees tpe 
            join main_employees_summary mes on 
            tpe.emp_id=mes.user_id 
            join tm_projects tp on tpe.project_id=tp.id 
            join tm_clients tc 
            on tp.client_id=tc.id 
            where tp.project_status = 'in-progress' 
            and mes.isactive=1 and tpe.is_active=1 
            and tp.project_name <> 'Management - Advisor' 
            order by mes.employeeId) query11
            inner join
            (select DISTINCT MONTHNAME(STR_TO_DATE(tts.ts_month, '%m')) as 'Report_Month' from tm_ts_status tts
            where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month < month(now())) query12
            ORDER by query11.employeeId)query1  
            LEFT JOIN  
            (SELECT mes.employeeId, tc.client_name, tp.project_name, tet.project_id, tp.project_code, tp.po_number, tp.sow_number,  
            tp.start_date, tp.end_date, tp.project_status, tp.project_type, 
            sum(if(tts.mon_status = 'Approved' or tts.mon_status = 'submitted',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved' or tts.tue_status = 'submitted',hour(tet.tue_duration),0))+ 
            sum(if(tts.wed_status = 'Approved' or tts.wed_status = 'submitted',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved' or tts.thu_status = 'submitted',hour(tet.thu_duration),0))+ 
            sum(if(tts.fri_status = 'Approved' or tts.fri_status = 'submitted',hour(tet.fri_duration),0)) as 'Billed_Hours',  
            MONTHNAME(STR_TO_DATE(tts.ts_month, '%m')) as 'Report_Month' from  
            tm_emp_timesheets tet join tm_ts_status tts on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and  
            tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week right join main_employees_summary mes on  
            tts.emp_id=mes.user_id join tm_projects tp on tts.project_id=tp.id join tm_clients tc on  
            tp.client_id=tc.id where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month < month(now()) 
            and mes.isactive = 1 and tet.project_task_id in (select id from tm_project_tasks tpt where task_id in (433,434)) and tp.project_type = 'billable' 
            group by mes.employeeId, tts.ts_month, tet.project_id order by mes.employeeId)query2 
            on query1.employeeId = query2.employeeId and query1.Report_Month = query2.Report_Month and query1.id = query2.project_id) query3 
            LEFT JOIN  
            (SELECT mes.employeeId, sum(if(tts.mon_status = 'Approved' or tts.mon_status = 'submitted',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved' or tts.tue_status = 'submitted',hour(tet.tue_duration),0))+ 
            sum(if(tts.wed_status = 'Approved' or tts.wed_status = 'submitted',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved' or tts.thu_status = 'submitted',hour(tet.thu_duration),0))+ 
            sum(if(tts.fri_status = 'Approved' or tts.fri_status = 'submitted',hour(tet.fri_duration),0)) as 'Leave_Hours',  
            MONTHNAME(STR_TO_DATE(tts.ts_month, '%m')) as 'Report_Month', tet.project_id from  
            tm_emp_timesheets tet join tm_ts_status tts on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and  
            tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week right join main_employees_summary mes on  
            tts.emp_id=mes.user_id join tm_projects tp on tts.project_id=tp.id join tm_clients tc on  
            tp.client_id=tc.id where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month < month(now()) 
            and mes.isactive = 1 and tet.project_task_id in (select id from tm_project_tasks tpt where task_id in (437,438,440,441,442,445)) 
            group by mes.employeeId, tts.ts_month, tet.project_id order by mes.employeeId) query4 
            on query3.employeeId = query4.employeeId and query3.Report_Month = query4.Report_Month and query3.id = query4.project_id)query5 
            LEFT JOIN 
            (SELECT mes.employeeId, sum(if(tts.mon_status = 'Approved' or tts.mon_status = 'submitted',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved' or tts.tue_status = 'submitted',hour(tet.tue_duration),0))+ 
            sum(if(tts.wed_status = 'Approved' or tts.wed_status = 'submitted',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved' or tts.thu_status = 'submitted',hour(tet.thu_duration),0))+ 
            sum(if(tts.fri_status = 'Approved' or tts.fri_status = 'submitted',hour(tet.fri_duration),0)) as 'Bench_Hours',  
            MONTHNAME(STR_TO_DATE(tts.ts_month, '%m')) as 'Report_Month', tet.project_id from  
            tm_emp_timesheets tet join tm_ts_status tts on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and  
            tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week right join main_employees_summary mes on  
            tts.emp_id=mes.user_id join tm_projects tp on tts.project_id=tp.id join tm_clients tc on  
            tp.client_id=tc.id where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month < month(now()) 
            and mes.isactive = 1 and (tet.project_task_id in (select id from tm_project_tasks tpt where task_id not in (433,434,437,438,440,441,442,445)) 
            or tp.project_type = 'non_billable') 
            group by mes.employeeId, tts.ts_month, tts.project_id order by mes.employeeId)query6 
            on query5.employeeId = query6.employeeId and query5.Report_Month = query6.Report_Month and query5.id = query6.project_id) query9
            LEFT JOIN  
            (SELECT mes.employeeId, sum(if(tts.mon_status = 'Approved' or tts.mon_status = 'submitted',1,0))+sum(if(tts.tue_status = 'Approved' or tts.tue_status = 'submitted',1,0))+ 
            sum(if(tts.wed_status = 'Approved' or tts.wed_status = 'submitted',1,0))+sum(if(tts.thu_status = 'Approved' or tts.thu_status = 'submitted',1,0))+ 
            sum(if(tts.fri_status = 'Approved' or tts.fri_status = 'submitted',1,0)) as 'Working_Days',  
            MONTHNAME(STR_TO_DATE(tts.ts_month, '%m')) as 'Report_Month', tet.project_id from  
            tm_emp_timesheets tet join tm_ts_status tts on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and  
            tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week right join main_employees_summary mes on  
            tts.emp_id=mes.user_id join tm_projects tp on tts.project_id=tp.id join tm_clients tc on  
            tp.client_id=tc.id where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month < month(now()) 
            and mes.isactive = 1 and tet.project_task_id in (select id from tm_project_tasks tpt where task_id not in (437,438,440,441,442,445)) 
            group by mes.employeeId, tts.ts_month, tet.project_id order by mes.employeeId) query10
            on query9.employeeId = query10.employeeId and query9.Report_Month = query10.Report_Month and query9.id = query10.project_id
            order by query9.employeeId;"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_employee_details(self):
        self.session = self.create_session()
        query = f"""SELECT query1.* FROM (  
                SELECT DISTINCT mes.employeeId 'Employee_ID',  
                mes.userfullname 'Employee_Name',  
                mes.emp_status_name as 'Employee_Status',  
                IF(mes.businessunit_name in ('Bangalore','Chennai','Hyderabad'),'IND', 'USA')  as Country,  
                mes.reporting_manager_name, mes.date_of_joining, mes.date_of_leaving,  mes.resigned_date,
                mes.modeofentry, mes.years_exp, 
                mes.primary_skill as 'Primary Skills',  
                tc.client_name 'Client_Name',  
                tp.project_name 'Project_Name',  
                tp.project_code 'Project_Code',  
                tp.project_status 'Project_Status',  
                tp.project_type 'Project_Type',  
                tp.po_number 'PO_Number',  
                tp.sow_number 'SOW_Number',  
                tp.po_value 'PO_Value',  
                tp.sow_value 'SOW_Value',  
                date_format(tp.start_date, '%Y-%m-%d') 'Project_Starts_Date',  
                date_format(tp.end_date, '%Y-%m-%d') 'Project_End_Date',  
                tpte.created,  
                tpte.created 'Project_Assignment_Date' FROM sentri.tm_project_task_employees tpte  
                join main_employees_summary mes on tpte.emp_id=mes.user_id  
                join tm_projects tp on tpte.project_id=tp.id  
                join tm_clients tc on tp.client_id=tc.id  
                where mes.isactive=1 and tpte.is_active=1  
                order by mes.employeeId) query1, (SELECT mes1.employeeId 'Employee_ID',  
                max(tpte1.created) 'Project_Assignment_Date' from main_employees_summary mes1  
                join tm_project_task_employees tpte1 on mes1.user_id = tpte1.emp_id  
                WHERE mes1.isactive=1 and tpte1.is_active=1  
                GROUP BY mes1.user_id) query2  
                WHERE query1.Employee_ID = query2.Employee_ID and query1.Project_Assignment_Date = query2.Project_Assignment_Date; """
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_task_details(self):
        self.session = self.create_session()
        query = f"""select DISTINCT employeeId,userfullname,date_of_joining,client_name,date_of_leaving,tm_emp_timesheets.ts_year ,tm_emp_timesheets.ts_month,
                    tm_emp_timesheets.sun_date,tm_emp_timesheets.mon_date,tm_emp_timesheets .tue_date,tm_emp_timesheets .wed_date,tm_emp_timesheets .thu_date,
                    tm_emp_timesheets .fri_date,tm_emp_timesheets .sat_date,sun_status,mon_status,tue_status,wed_status,thu_status,fri_status,sat_status,
                    tm_projects.project_name,tm_tasks.task
                    from tm_emp_timesheets 
                    inner join tm_ts_status on tm_emp_timesheets.emp_id = tm_ts_status.emp_id 
                    left join main_employees_summary  on tm_emp_timesheets.emp_id=main_employees_summary.user_id 
                    left join tm_projects  on tm_ts_status.project_id=tm_projects.id 
                    left join tm_clients  on tm_projects.client_id=tm_clients.id 
                    left join tm_project_tasks on tm_project_tasks.project_id =tm_emp_timesheets.project_id 
                    left join tm_tasks on tm_project_tasks.task_id =tm_tasks.id
                    where tm_project_tasks.task_id in (433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463)
                    and tm_emp_timesheets.ts_year in (2023,2022); """
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_bench_hours(self):
        self.session = self.create_session()
        query = f"""SELECT mes.employeeId, mes.userfullname, 
                mes.reporting_manager_name, tc.client_name, 
                tp.project_name, tp.project_code, 
                tp.po_number, tp.po_value, tp.sow_number, 
                tp.sow_value,
                date_format(tp.start_date, '%Y-%m-%d') as start_date,
                date_format(tp.end_date, '%Y-%m-%d') as end_date,  
                tp.project_status,
                sum(if(tts.mon_status = 'Approved',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved',hour(tet.tue_duration),0))+sum(if(tts.wed_status = 'Approved',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved',hour(tet.thu_duration),0))+sum(if(tts.fri_status = 'Approved',hour(tet.fri_duration),0)) as 'Bench_Hours', 
                MONTHNAME(STR_TO_DATE(month(DATE_SUB(now(), INTERVAL 1 month)), '%m')) as 'Month' 
                from tm_emp_timesheets tet 
                join tm_ts_status tts on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week 
                join main_employees_summary mes on tts.emp_id = mes.user_id 
                join tm_projects tp on tts.project_id = tp.id join tm_clients tc on tp.client_id=tc.id 
                where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month = month(DATE_SUB(now(), INTERVAL 1 month)) and mes.isactive = 1 and tet.project_task_id in (select id from tm_project_tasks tpt where task_id in (444,452)) group by mes.employeeId order by mes.employeeId;"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    
    def get_billed_and_leave_hours(self):
        self.session = self.create_session()
        query = f"""Select Query1.*, Query2.Leave_Hours from
            (SELECT mes.employeeId, mes.userfullname, mes.reporting_manager_name, tc.client_name, tp.project_name, tp.project_code, tp.po_number,
            tp.po_value, tp.sow_number, tp.sow_value, 
            date_format(tp.start_date, '%Y-%m-%d') as start_date,
            date_format(tp.end_date, '%Y-%m-%d') as end_date, 
            tp.project_status,
            sum(if(tts.mon_status = 'Approved',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved',hour(tet.tue_duration),0))+
            sum(if(tts.wed_status = 'Approved',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved',hour(tet.thu_duration),0))+
            sum(if(tts.fri_status = 'Approved',hour(tet.fri_duration),0)) as 'Billed_Hours', MONTHNAME(STR_TO_DATE(month(DATE_SUB(now(), INTERVAL 1 month)), '%m')) as 'Month'
            from tm_emp_timesheets tet join tm_ts_status tts on
            tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week
            right join main_employees_summary mes on tts.emp_id=mes.user_id
            join tm_projects tp on tts.project_id=tp.id
            join tm_clients tc on tp.client_id=tc.id
            where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month = month(DATE_SUB(now(), INTERVAL 1 month))
            and mes.isactive = 1 and tet.project_task_id in (select id from tm_project_tasks tpt where task_id in (433,434))
            group by mes.employeeId order by mes.employeeId) Query1,
            (SELECT mes.employeeId, mes.userfullname, mes.reporting_manager_name, tc.client_name, tp.project_name, tp.project_code, tp.po_number,
            tp.po_value, tp.sow_number, tp.sow_value, tp.start_date, tp.end_date, tp.project_status,
            sum(if(tts.mon_status = 'Approved',hour(tet.mon_duration),0))+sum(if(tts.tue_status = 'Approved',hour(tet.tue_duration),0))+
            sum(if(tts.wed_status = 'Approved',hour(tet.wed_duration),0))+sum(if(tts.thu_status = 'Approved',hour(tet.thu_duration),0))+
            sum(if(tts.fri_status = 'Approved',hour(tet.fri_duration),0)) as 'Leave_Hours', MONTHNAME(STR_TO_DATE(month(DATE_SUB(now(), INTERVAL 1 month)), '%m')) as 'Month'
            from tm_emp_timesheets tet join tm_ts_status tts on
            tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week
            right join main_employees_summary mes on tts.emp_id=mes.user_id
            join tm_projects tp on tts.project_id=tp.id
            join tm_clients tc on tp.client_id=tc.id
            where tts.ts_year = year(DATE_SUB(now(), INTERVAL 1 month)) and tts.ts_month = month(DATE_SUB(now(), INTERVAL 1 month))
            and mes.isactive = 1 and tet.project_task_id in (select id from tm_project_tasks tpt where task_id in (437,438,440,441,442,445))
            group by mes.employeeId order by mes.employeeId) Query2
            where Query1.employeeId = Query2.employeeId;"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_active_projects(self):
        self.session = self.create_session()
        query = f"""SELECT tc.client_name 'Client Name', 
                    tp.project_name 'Project Name', 
                    tp.project_code 'Project Code',
                    tp.project_status 'Project Status', 
                    tp.project_type 'Project Type', 
                    tp.sow_number 'SOW Number', 
                    tp.po_number 'PO Number', 
                    tp.start_date 'Project Starts Date', 
                    tp.end_date 'Project End Date' from tm_projects tp 
                    join tm_clients tc on tp.client_id = tc.id 
                    where tp.project_status = 'in-progress' order by tc.client_name, tp.project_name;"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
  

    def get_historic_timesheet(self):
        self.session = self.create_session()
        query = "select * from tm_ts_defaulters"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def timesheet_defaulters_list(self):
        self.session = self.create_session()
        query = f"""select mes.employeeId, mes.userfullname, mes.reporting_manager_name , mes.date_of_joining, mes.date_of_leaving, tts.ts_year, tts.ts_month,
            sum((if(tts.mon_status = 'submitted',hour(tet.mon_duration),0))+(if(tts.tue_status = 'submitted',hour(tet.tue_duration),0))+ 
            (if(tts.wed_status = 'submitted',hour(tet.wed_duration),0))+(if(tts.thu_status = 'submitted',hour(tet.thu_duration),0))+ 
            (if(tts.fri_status = 'submitted',hour(tet.fri_duration),0))) as 'submitted',
            sum((if(tts.mon_status = 'approved',hour(tet.mon_duration),0))+(if(tts.tue_status = 'approved',hour(tet.tue_duration),0))+ 
            (if(tts.wed_status = 'approved',hour(tet.wed_duration),0))+(if(tts.thu_status = 'approved',hour(tet.thu_duration),0))+ 
            (if(tts.fri_status = 'approved',hour(tet.fri_duration),0))) as 'approved',
            CASE
                WHEN month(mes.date_of_joining) = month(DATE_SUB(now(), INTERVAL 1 month)) and year(mes.date_of_joining) = year(DATE_SUB(now(), INTERVAL 1 month)) 
                THEN (select 5 * (DATEDIFF(last_day(DATE_SUB(now(), INTERVAL 1 month)), mes.date_of_joining) DIV 7) 
                + MID('1234555512344445123333451222234511112345001234550', 
                7 * WEEKDAY(mes.date_of_joining) + WEEKDAY(last_day(DATE_SUB(now(), INTERVAL 1 month))) + 1, 1))
                WHEN month(mes.date_of_leaving) = month(DATE_SUB(now(), INTERVAL 1 month)) and year(mes.date_of_leaving) = year(DATE_SUB(now(), INTERVAL 1 month))
                THEN (select 5 * (DATEDIFF(mes.date_of_leaving, DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) DIV 7) 
                + MID('1234555512344445123333451222234511112345001234550', 
                7 * WEEKDAY(DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) + WEEKDAY(mes.date_of_leaving) + 1, 1))
                ELSE (select 5 * (DATEDIFF(last_day(DATE_SUB(now(), INTERVAL 1 month)), DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) DIV 7) 
                + MID('1234555512344445123333451222234511112345001234550',
                7 * WEEKDAY(DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) + WEEKDAY(last_day(DATE_SUB(now(), INTERVAL 1 month))) + 1, 1))
            END as 'expected_days'
            from 
            main_employees_summary mes join tm_emp_timesheets tet on mes.user_id = tet.emp_id
            join tm_ts_status tts on tet.emp_id = tts.emp_id and tet.project_id = tts.project_id and  
            tet.ts_year = tts.ts_year and tet.ts_month = tts.ts_month and tet.ts_week = tts.ts_week
            and tts.ts_year = year(now()) and tts.ts_month = month(DATE_SUB(now(), INTERVAL 1 month))
            group by mes.employeeId"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_timesheet_defaulters(self):
        self.session = self.create_session()
        query = f"""select mes.employeeId, mes.userfullname, mes.reporting_manager_name,mes.date_of_joining, mes.date_of_leaving,
            year(now()) as ts_year, month(DATE_SUB(now(), INTERVAL 1 month)) as ts_month, '0' as 'submitted', '0' as 'approved',
            CASE
                WHEN month(mes.date_of_joining) = month(DATE_SUB(now(), INTERVAL 1 month)) and year(mes.date_of_joining) = year(DATE_SUB(now(), INTERVAL 1 month)) 
                THEN (select 5 * (DATEDIFF(last_day(DATE_SUB(now(), INTERVAL 1 month)), mes.date_of_joining) DIV 7) 
                + MID('1234555512344445123333451222234511112345001234550', 
                7 * WEEKDAY(mes.date_of_joining) + WEEKDAY(last_day(DATE_SUB(now(), INTERVAL 1 month))) + 1, 1))
                WHEN month(mes.date_of_leaving) = month(DATE_SUB(now(), INTERVAL 1 month)) and year(mes.date_of_leaving) = year(DATE_SUB(now(), INTERVAL 1 month))
                THEN (select 5 * (DATEDIFF(mes.date_of_leaving, DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) DIV 7) 
                + MID('1234555512344445123333451222234511112345001234550', 
                7 * WEEKDAY(DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) + WEEKDAY(mes.date_of_leaving) + 1, 1))
                ELSE (select 5 * (DATEDIFF(last_day(DATE_SUB(now(), INTERVAL 1 month)), DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) DIV 7) 
                + MID('1234555512344445123333451222234511112345001234550',
                7 * WEEKDAY(DATE_ADD(last_day(DATE_SUB(DATE_SUB(now(), INTERVAL 1 month), INTERVAL 1 month)), INTERVAL 1 day)) + WEEKDAY(last_day(DATE_SUB(now(), INTERVAL 1 month))) + 1, 1))
            END as 'expected_days'
            from main_employees_summary mes
            where user_id not in (select distinct tts.emp_id from tm_ts_status tts where 
            tts.ts_year = year(now()) and tts.ts_month = month(DATE_SUB(now(), INTERVAL 1 month))) and 
            ((month(mes.date_of_leaving) =  month(DATE_SUB(now(), INTERVAL 1 month)) and year(mes.date_of_leaving) =  year(DATE_SUB(now(), INTERVAL 1 month)))
            or mes.date_of_leaving is null)"""
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_all_departments(self):
        self.session = self.create_session()
        query = "SELECT id, deptname FROM sentri.main_departments where isactive=1"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result
    
    def get_all_business_unit(self):
        self.session = self.create_session()
        query = "SELECT id, unitname FROM sentri.main_businessunits where isactive=1"
        result = self.get_executed_result(query)
        self.close_database_connection()
        return result

    def get_executed_result(self, query):
        executed_session = self.session.execute(query)
        result = self.populate_list(executed_session)
        return result
    
    def create_engine(self):
        db_user = 'remote_user'
        db_pass = 'remote_user'
        db_name = 'sentri'
        db_hostname = '35.185.113.63'
        db_port = '3306'

        pool = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername="mysql+pymysql",
                username=db_user,
                password=db_pass,
                host=db_hostname,
                port=db_port,
                database=db_name,
            )
        )
        return pool
    
    def populate_list(self, rows):
        result = list()
        for row in rows:
            result.append(dict(row))
        return result

