## Author: Steve S.
## Date: 11/2022
## Script updates derived records request fcs for viewing in dashboards of one-to-many
## relationship of request to response depts.

import os
import sys
import time
import shutil
import arcpy
from datetime import datetime, date, timedelta
##from pandas.tseries.holiday import USFederalHolidayCalendar
import pytz
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
##from office365.runtime.auth.user_credential import UserCredential
##from office365.runtime.auth.authentication_context import AuthenticationContext
##from office365.sharepoint.client_context import ClientContext
##from office365.sharepoint.files.file import File

def main():

    # general variables
    global now, this_path, this_file, logfile, this_logger, processing_gdb
    now = datetime.now()
    full_path = os.path.realpath(__file__)
    this_path = os.path.dirname(full_path)
    this_file = os.path.basename(full_path)

    # set and start logfile
    logfile = this_path + '\\logs\\{}_{}_{}_{}_{}_{}.log'.format(str(now.year), str(now.month), str(now.day), str(now.hour), str(now.minute), str(now.second))
    fhFormatter = logging.Formatter('%(asctime)-12s %(funcName)-36s %(message)-96s', '%I:%M:%S %p')
    fh_debug = logging.FileHandler(logfile, 'w')
    fh_debug.setLevel(logging.DEBUG)
    fh_debug.setFormatter(fhFormatter)
    this_logger = logging.getLogger('this_logger')
    this_logger.setLevel(logging.DEBUG)
    this_logger.addHandler(fh_debug)
    this_logger.debug("script started")

    # Get current Mountain time
    tz_den = pytz.timezone('America/Denver')
    current_den_time = str(datetime.now(tz_den))
    # this_logger.debug(str(current_den_time))
    current_den_time_reformatted = datetime.strptime(current_den_time[:19], '%Y-%m-%d %H:%M:%S')
    # this_logger.debug(current_den_time_reformatted)
    current_utc = datetime.utcnow()
    # this_logger.debug("Current UTC time: " + str(current_utc))

    # data variables
    # inputs and derivative
    input_xlsx_dir = "\\\\dummy_path"
    processing_gdb_dir = this_path + "\\processing_gdbs"
    output_xlsx_dir = this_path + "\\cora_spreadsheets\\working"
    sde_prod_connection = this_path + "\\db_connections\\ap-sql3@CD_Prod@OS_Authentication.sde"
    requests_fc = sde_prod_connection + "\\CD_Prod.CD_GIS.OpenRecordsRequests"
    requests_by_dept_fc = sde_prod_connection + "\\CD_Prod.CD_GIS.OpenRecordsRequestsByResponseDept"
    requests_by_div_fc = sde_prod_connection + "\\CD_Prod.CD_GIS.OpenRecordsRequestsByResponseDiv"
    requests_fc_field_names = ['OBJECTID', 'RequestID', 'RequestNo', 'InitiatingEntity', 'RequestDate', 'DueDate', 'DateClosed', 'RequestedBy', 'PressMedia', 'RequestSummary', 'Address1', 'Address2', 'ParcelPin1', 'ParcelPin2', 'ClarificationNotes', 'ResponseDepts', 'ResponseDivs', 'RecordsProvided', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest', 'Notes', 'Hyperlink', 'InitialDeposit', 'InitialDepositDate', 'FinalBalance', 'FinalBalanceDate', 'TotalPaid', 'DaysProcessing']
    requests_by_dept_fc_field_names = ['OBJECTID', 'RequestID', 'RequestNo', 'InitiatingEntity', 'RequestDate', 'DueDate', 'DateClosed', 'RequestedBy', 'PressMedia', 'RequestSummary', 'Address1', 'Address2', 'ParcelPin1', 'ParcelPin2', 'ClarificationNotes', 'ResponseDept', 'RecordsProvided', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest', 'Notes', 'Hyperlink', 'MonthInitiated', 'ResponseDivs']
    requests_by_div_fc_field_names = ['OBJECTID', 'RequestID', 'RequestNo', 'InitiatingEntity', 'RequestDate', 'DueDate', 'DateClosed', 'RequestedBy', 'PressMedia', 'RequestSummary', 'Address1', 'Address2', 'ParcelPin1', 'ParcelPin2', 'ClarificationNotes', 'ResponseDept', 'RecordsProvided', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest', 'Notes', 'Hyperlink', 'MonthInitiated', 'ResponseDiv']
    # requests_by_dept_list_fc_field_names = ['OBJECTID', 'RequestID', 'InitiatingEntity', 'RequestNo', 'RequestDate', 'DueDate', 'DateClosed', 'RequestedBy', 'RequestSummary', 'RequestClarification', 'ResponseDeptList', 'RecordsProvided', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest', 'RequestRoute', 'Notes', 'Hyperlink', 'created_user', 'created_date', 'last_edited_user', 'last_edited_date', 'SHAPE', 'GlobalID', 'month_initiated', 'ResponseDeptsAutoNotified', 'ResponseDivs1AutoNotified', 'ResponseDivs2AutoNotified', 'ResponseDivs3AutoNotified', 'ResponseDivs4AutoNotified']

    # function calls
    # xlsx_file = download_xlsx()
    delete_old_files_or_folders(output_xlsx_dir)
    delete_old_files_or_folders(processing_gdb_dir)
    output_tbl = process_xlsxs_into_table(input_xlsx_dir, processing_gdb_dir, output_xlsx_dir)
    groom_output_tbl(output_tbl, ['OBJECTID', 'RequestID', 'RequestNo', 'DueDate', 'ResponseDepts', 'ResponseDivs', 'InitiatingEntity', 'RequestDate', 'DateClosed', 'InitialDeposit', 'InitialDepositDate', 'FinalBalance', 'FinalBalanceDate', 'TotalPaid', 'DaysProcessing', 'PressMedia', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest'])
    update_copy(output_tbl, sde_prod_connection, requests_fc.split("\\")[-1])
    update_derivative_fcs(requests_fc, requests_fc_field_names, requests_by_dept_fc, requests_by_dept_fc_field_names, requests_by_div_fc, requests_by_div_fc_field_names)

    this_logger.debug("script complete")
    send_log_email(["steve.sanford@thorntonco.gov"])

def delete_old_files_or_folders(in_dir):
    # deletes all existing files or folders not created at 6:00pm hour and optionally files and folders created beyond 4 weeks ago
    try:
        now = time.time()
        non_biz_hrs_list = ["19", "20", "21", "22", "23", "0", "1", "2", "3", "4", "5", "6", "7"]
        for f in os.listdir(in_dir):
            # this_logger.debug("{} created {}".format(str(f), str(os.stat(os.path.join(in_dir, f)).st_mtime)))
            # this_logger.debug("compared with " + str(now - 28*86400))
            # delete item if not made during business hours 800-1800
            if (f.split("_")[-3]) in non_biz_hrs_list:
                if os.path.isfile(os.path.join(in_dir, f)):
                    this_logger.debug("deleting {}".format(str(f)))
                    os.remove(os.path.join(in_dir, f))
                elif os.path.isdir(os.path.join(in_dir, f)):
                    this_logger.debug("deleting {}".format(str(f)))
                    shutil.rmtree(os.path.join(in_dir, f))
            # delete item if modified date older than 21 days, except file gdbs
            elif os.stat(os.path.join(in_dir, f)).st_mtime < now - 15*86400:
                if os.path.isfile(os.path.join(in_dir, f)):
                    this_logger.debug("deleting {}".format(str(f)))
                    os.remove(os.path.join(in_dir, f))
##                # do not use below for file gdbs as their modified time is updated whenever folder is accessed. use ctime instead.
##                elif os.path.isdir(os.path.join(in_dir, f)):
##                    this_logger.debug("deleting {}".format(str(f)))
##                    # shutil.rmtree(os.path.join(in_dir, f))
            # delete item if creation date is older than 21 days
            elif os.stat(os.path.join(in_dir, f)).st_ctime < now - 90*86400:
                if os.path.isdir(os.path.join(in_dir, f)):
                    this_logger.debug("deleting {}".format(str(f)))
##                    this_logger.debug("creation time {}".format(str(os.stat(os.path.join(in_dir, f)).st_ctime)))
##                    this_logger.debug("time four days ago {}".format(str(now - 28*86400)))
                    shutil.rmtree(os.path.join(in_dir, f))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def update_copy(in_tbl, out_dir, out_tbl):

    try:

        # this is necessary due to a string to date conversion bringing in null DateClosed values into a Date field as 1899 date
        # only happens when only no date is entered into a field intended for a date, brought into GIS as a string
        this_logger.debug("beginning blank date field adjustments...")
        with arcpy.da.UpdateCursor(in_tbl, ["OBJECTID", "DateClosed", "DueDate"]) as ucursor:
            for row in ucursor:
                # this_logger.debug("ClosedDate is " + str(row[1]))
                if str(row[1]) == "1899-12-30 00:00:00":
                    row[1] = None
                    this_logger.debug("OBJECTID {}'s DateClosed updated to None from auto-generated {} (string to date conversion)".format(str(row[0]), str(row[1])))
                # this_logger.debug("ClosedDate is " + str(row[1]))
                if (str(row[2]) == "1899-12-30 00:00:00") or ("2099-12-31" in str(row[2])):
                    row[2] = None
                    this_logger.debug("OBJECTID {}'s DueDate updated to None from auto-generated {} (string to date conversion)".format(str(row[0]), str(row[2])))
                ucursor.updateRow(row)
        this_logger.debug("completed blank date field adjustments. proceeding to truncate of main prod table.")
        arcpy.TruncateTable_management(out_dir + "\\" + out_tbl)
        this_logger.debug("truncated prod copy of {}".format(out_tbl))
        arcpy.Append_management(in_tbl, out_dir + "\\" + out_tbl, "NO_TEST", "#", "#")
        this_logger.debug("appended new {} from processing copy".format(str(out_tbl)))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def download_xlsx_from_sharepoint():

    try:

        # variables
        # sharepoint_base_url = 'https://cityofthornton-my.sharepoint.com/personal/reese_evenson_thorntonco_gov'
        sharepoint_base_url = 'https://cityofthornton-my.sharepoint.com/personal/steve_sanford_thorntonco_gov'
        sharepoint_user = 'steve.sanford@thorntonco.gov'
        sharepoint_password = '#'

        # authenticiate into sharepoint
        auth = AuthenticationContext(sharepoint_base_url)
        auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
        ctx = ClientContext(sharepoint_base_url, auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        this_logger.debug('logged into SharePoint')

##        # get list of files in provided folder
##        # should be own folder, not another user's
##        folder_in_sharepoint = 'Documents/'
##        def folder_details(ctx, folder_in_sharepoint):
##            folder = ctx.web.get_folder_by_server_relative_url(folder_in_sharepoint)
##            fold_names = []
##            sub_folders = folder.files
##            ctx.load(sub_folders)
##            ctx.execute_query()
##            for s_folder in sub_folders:
##                fold_names.append(s_folder.properties["Name"])
##            return fold_names
##        file_list = folder_details(ctx, folder_in_sharepoint)
##        this_logger.debug("Files in folder: " + str(file_list))

        # read file
        # sharepoint_file = '/personal/reese_evenson_thorntonco_gov/Documents/GIS%20Test%20Data/Open%20Records%20Requests%20GIS%20Data%20Input%20Table.xlsx'
        sharepoint_file = '/personal/steve_sanford_thorntonco_gov/Documents/cora.xlsx'
        file_response = File.open_binary(ctx, sharepoint_file)

        # save file
        with open(this_path + "\\cora_spreadsheets\\active\\current_cora.xlsx", 'wb') as output_file:
            output_file.write(file_response.content)
            this_logger.debug("downloaded sharepoint file " + str(sharepoint_file.split("/")[-1]))

        return output_file

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def process_xlsxs_into_table(xlsx_input_dir, processing_gdb_dir, output_xlsx_dir):

    try:
        # create file gdb
        processing_gdb_name = "{}_{}_{}_{}_{}_{}.gdb".format(str(now.year), str(now.month), str(now.day), str(now.hour), str(now.minute), str(now.second))
        arcpy.CreateFileGDB_management(processing_gdb_dir, processing_gdb_name)
        this_logger.debug("created processing gdb: " + str(processing_gdb_name))

##        # delete contents of local active folder
##        for root, dirs, files in os.walk(this_path + "\\cora_spreadsheets\\working"):
##            for f in files:
##                os.unlink(os.path.join(root, f))

        # copy tables from share to local, add tables per excel file, add to merge list as you go
        # xlsx_input_dir = "\\\\it-filesrv\\shared\\ArcGIS\\cora_testing\\working"
        # exclude items that aren't files and files that are lock files (~ in their names)
        xlsx_file_list = [f for f in os.listdir(xlsx_input_dir) if (os.path.isfile(os.path.join(xlsx_input_dir, f))) and ("~" not in str(f)) and (".tmp" not in str(f)) and (".docx" not in str(f)) and (".db" not in str(f))]
        this_logger.debug("xlsx file list: " + str(xlsx_file_list))
        output_tbl_list = []
        # copy to have backup and snapshot of data in time independent of editing copy on Share
        for xlsx in xlsx_file_list:
            output_tbl_name = str(xlsx).split(".")[0] + "_" + str(processing_gdb_name).split(".")[0]
            this_logger.debug("copying {} locally to avoid locking issue on Share".format(str(xlsx)))
            shutil.copyfile(os.path.join(xlsx_input_dir, xlsx), output_xlsx_dir + "\\" + str(xlsx).split(".")[0] + "_" + str(processing_gdb_name).split(".")[0] + ".xlsm")
            shutil.copyfile(os.path.join(xlsx_input_dir, xlsx), output_xlsx_dir + "\\" + str(xlsx).split(".")[0] + "_" + str(processing_gdb_name).split(".")[0] + ".xlsx")
            arcpy.ExcelToTable_conversion(output_xlsx_dir + "\\" + str(xlsx).split(".")[0] + "_" + str(processing_gdb_name).split(".")[0] + ".xlsx", os.path.join(processing_gdb_dir, processing_gdb_name, output_tbl_name))
            this_logger.debug("processed copied xlsx {} into processing gdb {}".format(str(xlsx), str(processing_gdb_name)))
            output_tbl_list.append(os.path.join(processing_gdb_dir, processing_gdb_name, output_tbl_name))

        # merge into one table, return
        # this_logger.debug("merging tables: " + str(output_tbl_list))
        output_tbl = arcpy.Merge_management(output_tbl_list, os.path.join(processing_gdb_dir, processing_gdb_name, "output_tbl"))
        return output_tbl

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def groom_output_tbl(tbl, flds):

    try:

        # add RequestID field
        arcpy.AddField_management(tbl, "RequestID", "TEXT", "#", "#", 25, "Request ID")
        this_logger.debug("added RequestID field")

        # add TotalPaid field
        arcpy.AddField_management(tbl, "TotalPaid", "DOUBLE", "#", "#", "#", "Total Paid")
        this_logger.debug("added TotalPaid field")

        # add TotalPaid field
        arcpy.AddField_management(tbl, "DaysProcessing", "DOUBLE", "#", "#", "#", "Days Processing")
        this_logger.debug("added DaysProcessing field")

        # cursor through and update certain values
        # fields: [0'OBJECTID', 1'RequestID', 2'RequestNo', 3'DueDate', 4'ResponseDepts', 5'ResponseDivs', 6'InitiatingEntity', 7'RequestDate',
        # 8'DateClosed', 9'InitialDeposit', 10'InitialDepositDate', 11'FinalBalance', 12'FinalBalanceDate', 13'TotalPaid', 14'DaysProcessing',
        # 15'PressMedia, 16'PaymentRequired', 17'ExtensionRequired', 18'InformationRequest']
        with arcpy.da.UpdateCursor(tbl, flds) as ucursor:
            for row in ucursor:
                # if no RequestDate, delete
                nulls_list = ["", "None", "1899-12-30 00:00:00"]   # can be any of these depending on what records already exist in xlsm and how field is brought into gis--date, string, etc.
                if str(row[7]) in nulls_list:
                    ucursor.deleteRow()
                    this_logger.debug("OBJECTID {} deleted as RequestDate is {}".format(str(row[0]), str(row[7])))
                else:
                    # sort response depts
                    response_dept_list = str(row[4]).split(", ")
                    response_dept_list = sorted(response_dept_list)
                    row[4] = ", ".join(item for item in response_dept_list)
                    # sort response divs
                    response_div_list = str(row[5]).split(", ")
                    response_div_list = sorted(response_div_list)
                    row[5] = ", ".join(item for item in response_div_list)
                    # if any blank values exist in DueDate, set them to 2099 date
                    if (str(row[3]) == "None"):
                        row[3] = date(2099, 12, 31)
                        this_logger.debug("OBJECTID {}'s DueDate updated to 12/31/2099 from None".format(str(row[0])))
                    # if no values exist in ClosedDate field to force Date type when brought into ArcGIS, set them back to Null
                    # this is handled with an after-the-fact ucursor when updating sde (update_copy function)
##                    this_logger.debug("DateClosed is " + str(row[8]))
##                    if (str(row[8])) in nulls_list:
##                        row[8] = date(None)
##                        this_logger.debug("OBJECTID {}'s DateClosed updated to None from one of null string values".format(str(row[0])))
                    # calculate RequestID
                    if (str(row[2]) != "None"):
                        row[1] = str(now.year) + "-" + str(row[6]) + "-" + str(row[2])
                    else:
                        row[1] = str(now.year) + "-" + str(row[6]) + "-#"
                    # calculate TotalPaid field
                    # if only finalbalance fields populated, totalpaid is finalbalance (date field used to indicate payment actually received and want to make sure there is a final balance field to calculate)
                    if (str(row[12]) not in nulls_list) and (str(row[11]) not in nulls_list) and (str(row[10]) in nulls_list) and (str(row[9]) in nulls_list):
                        row[13] = float(row[11])
                        this_logger.debug("OBJECTID {}'s TotalPaid set to final balance {}".format(str(row[0]), str(row[13])))
                    # else if only deposit fields populated, totalpaid is deposit
                    elif (str(row[12]) in nulls_list) and (str(row[11]) in nulls_list) and (str(row[10]) not in nulls_list) and (str(row[9]) not in nulls_list):
                        row[13] = float(row[9])
                        this_logger.debug("OBJECTID {}'s TotalPaid set to deposit {}".format(str(row[0]), str(row[9])))
                    # else if only deposit fields populated and finalbalance entered but no finalbalance date entered, totalpaid is deposit
                    elif (str(row[12]) in nulls_list) and (str(row[11]) not in nulls_list) and (str(row[10]) not in nulls_list) and (str(row[9]) not in nulls_list):
                        row[13] = float(row[9])
                        this_logger.debug("OBJECTID {}'s TotalPaid set to deposit {}, FinalBalance value exists but no FinalBalanceDate".format(str(row[0]), str(row[9])))
                    # else if both deposit and finalbalance received (and dates entered), calculate sum for totalpaid
                    elif (str(row[12]) not in nulls_list) and (str(row[11]) not in nulls_list) and (str(row[10]) not in nulls_list) and (str(row[9]) not in nulls_list):
                        row[13] = float(row[9]) + float(row[11])
                        this_logger.debug("OBJECTID {}'s TotalPaid set to final balance {} + deposit {}".format(str(row[0]), str(row[11]), str(row[9])))

                    # None-out 0s and December 1899s for $ fields
                    if str(row[9]) in nulls_list:
                        row[9] = None
                        row[10] = None
                    if str(row[11]) in nulls_list:
                        row[11] = None
                        row[12] = None
                    # calculate days processing field
                    if str(row[8]) not in nulls_list:
                        # cal = USFederalHolidayCalendar()
                        # holidays = cal.holidays(start=datetime(2023, 1, 1), end=datetime(2023, 12, 31))
                        # this_logger.debug("holidays: " + str(holidays))
                        holidays = ['2023-01-02 00:00:00', '2023-01-16 00:00:00', '2023-02-20 00:00:00', '2023-05-29 00:00:00',
                        '2023-06-19 00:00:00', '2023-07-04 00:00:00', '2023-09-04 00:00:00', '2023-11-10 00:00:00', '2023-11-23 00:00:00',
                        '2023-11-24 00:00:00', '2023-12-25 00:00:00']
                        days_processing = 0
                        if row[8] > row[7]:
                            this_date = row[8]
                            # this_logger.debug("this date: " + str(this_date))
                            while this_date != row[7]:
                                this_date = this_date - timedelta(days=1)
                                day_of_week = datetime.date(this_date).weekday()  # weekday function reference: mon-0, tues-1, wed-2, thurs-3, fri-4, sat-5, sun-6
                                if (day_of_week != 5) and (day_of_week != 6) and (str(this_date) not in holidays):
                                    days_processing += 1
                        elif row[8] == row[7]:
                            days_processing = 0
                        elif row[8] < row[7]:   # negative value occurs if DateClosed is for instance typo'd last year
                            days_processing = 0
                        row[14] = days_processing
                        this_logger.debug("OBJECTID {} spent {} business days processing".format(str(row[0]), str(days_processing)))
                    # set PressMedia to N if empty
                    if str(row[15]) in nulls_list:
                        row[15] = "N"
                        this_logger.debug("OBJECTID {}'s PressMedia updated to N from None".format(str(row[0])))
                    # set PaymentRequired to N if empty
                    if str(row[16]) in nulls_list:
                        row[16] = "N"
                        this_logger.debug("OBJECTID {}'s PaymentRequired updated to N from None".format(str(row[0])))
                    # set ExtensionRequired to N if empty
                    if str(row[17]) in nulls_list:
                        row[17] = "N"
                        this_logger.debug("OBJECTID {}'s ExtensionRequired updated to N from None".format(str(row[0])))
                    # set InformationRequest to N if empty
                    if str(row[18]) in nulls_list:
                        row[18] = "N"
                        this_logger.debug("OBJECTID {}'s InformationRequest updated to N from None".format(str(row[0])))
                    ucursor.updateRow(row)
        this_logger.debug("completed grooming")

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def get_this_response_depts_response_divs_list(response_dept, response_divs_list):

    try:
        this_response_depts_response_divs_list = []
        for item in response_divs_list:
            if response_dept == item.split("/")[0]:
                this_response_depts_response_divs_list.append(item)
        this_response_depts_response_divs_list = sorted(this_response_depts_response_divs_list)
        return this_response_depts_response_divs_list

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def ensure_response_div_is_in_response_dept_list(response_div, response_dept_list):

    try:
        if response_div in response_dept_list:
            # this_logger.debug("{} is in response div list {}, returning True to proceed...".format(str(response_div), str(response_dept_list)))
            return True
        else:
            # this_logger.debug("{} is NOT in response div list, returning False, will not proceed with insert...".format(str(response_div), str(response_dept_list)))
            return False

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def update_derivative_fcs(requests_fc, requests_fc_field_names, requests_by_dept_fc, requests_by_dept_fc_field_names, requests_by_div_fc, requests_by_div_fc_field_names):

    try:

        # now process data to derivative fcs for dashboard
        # first truncate all derivative fcs
        this_logger.debug("processing data for derivative fcs...")
        for fc in [requests_by_dept_fc, requests_by_div_fc]:
            arcpy.TruncateTable_management(fc)
            this_logger.debug("truncated {} fc".format(str(fc.split(".")[-1])))

        # insert records into derivative fcs
        with arcpy.da.SearchCursor(requests_fc, requests_fc_field_names, sql_clause=(None,'ORDER BY RequestID ASC')) as scursor:
            for row in scursor:

                # define value variables
                # requests fc: [0'OBJECTID', 1'RequestID', 2'RequestNo', 3'InitiatingEntity', 4'RequestDate', 5'DueDate', 6'DateClosed',
                # 7'RequestedBy', 8'Press', 9'RequestSummary', 10'Address1', 11'Address2', 12'ParcelPin1', 13'ParcelPin2', 14'RequestClarification',
                # 15'ResponseDepts', 16'ResponseDivs', 17'RecordsProvided', 18'PaymentRequired', 19'ExtensionRequired', 20'InformationRequest',
                # 21'Notes', 22'Hyperlink', 'InitialDeposit', 'InitialDepositDate', 'FinalBalance', 'FinalBalanceDate', 'TotalPaid']
                oid = row[0]
                request_id = row[1]
                request_no = row[2]
                initiating_entity = row[3]
                request_date = row[4]
                due_date = row[5]
                date_closed = row[6]
                requested_by = row[7]
                press = row[8]
                request_summary = row[9]
                address_1 = row[10]
                address_2 = row[11]
                parcel_pin_1 = row[12]
                parcel_pin_2 = row[13]
                request_clarification = row[14]
                response_depts = row[15]
                response_divs = row[16]
                records_provided = row[17]
                payment_required = row[18]
                extension_required = row[19]
                information_request = row[20]
                notes = row[21]
                hyperlink = row[22]

                # calculate month_initiated so that by month works for distinct in dashboard serial chart (did not work with date fields well)
                # request_date_formatted = datetime.strptime(str(request_date), "%Y-%m-%d %H:%M:%S")
                request_month_int = request_date.month

                this_logger.debug("evaluating request ID {}...".format(str(request_id)))
                # create list of response depts, get count
                response_depts_list = list(str(response_depts).split(", "))
                this_logger.debug("response dept list: " + str(response_depts_list))
                response_dept_count = len(response_depts_list)
                this_logger.debug("response dept list count: " + str(response_dept_count))

                response_divs_list = list(str(response_divs).split(", "))
                this_logger.debug("response div list: " + str(response_divs_list))
                response_div_count = len(response_divs_list)
                this_logger.debug("response div list count: " + str(response_div_count))

                # insert into by dept fc, based on number of response depts for request, one per response dept per request
                # by dept fc fields: ['OBJECTID', 'RequestID', 'RequestNo', 'InitiatingEntity', 'RequestDate', 'DueDate', 'DateClosed', 'RequestedBy', 'Press', 'RequestSummary', 'Address1', 'Address2', 'ParcelPin1', 'ParcelPin2', 'RequestClarification', 'ResponseDept', 'RecordsProvided', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest', 'Notes', 'Hyperlink', 'MonthInitiated', 'ResponseDivs']
                icursor_by_dept = arcpy.da.InsertCursor(requests_by_dept_fc, requests_by_dept_fc_field_names)
                if response_depts_list == ['']:
                    this_logger.debug("By Dept fc: request id {} has no response dept, no record insertion, moving on to next record".format(str(request_id)))
                    del icursor_by_dept
                    continue
                i=1
                while i <= response_dept_count:
                    # this_logger.debug("i is {}".format(str(i)))
                    # get list of response divs just for tht response dept
                    this_response_depts_response_divs_list = get_this_response_depts_response_divs_list(response_depts_list[i-1].strip(), response_divs_list)
                    icursor_by_dept.insertRow((oid, request_id, request_no, initiating_entity, request_date, due_date, date_closed, requested_by, press, request_summary, address_1, address_2, parcel_pin_1, parcel_pin_2, request_clarification, response_depts_list[i-1].strip(), records_provided, payment_required, extension_required, information_request, notes, hyperlink, request_month_int, ", ".join(item for item in this_response_depts_response_divs_list)))
                    this_logger.debug("By Dept fc: request id {} inserted for response dept {}".format(str(request_id), str(response_depts_list[i-1])))
                    i+=1
                del icursor_by_dept

                # now insert into by div fc, based on number of response divs for request, one per response div per request
                # by div fc fields: ['OBJECTID', 'RequestID', 'RequestNo', 'InitiatingEntity', 'RequestDate', 'DueDate', 'DateClosed', 'RequestedBy', 'PressMedia', 'RequestSummary', 'Address1', 'Address2', 'ParcelPin1', 'ParcelPin2', 'ClarificationNotes', 'ResponseDept', 'RecordsProvided', 'PaymentRequired', 'ExtensionRequired', 'InformationRequest', 'Notes', 'Hyperlink', 'MonthInitiated', 'ResponseDiv']
                icursor_by_div = arcpy.da.InsertCursor(requests_by_div_fc, requests_by_div_fc_field_names)
                if response_divs_list == ['']:
                    this_logger.debug("By Div fc: request id {} has no response div, no record insertion".format(str(request_id)))
                    del icursor_by_div
                    continue
                i=1
                while i <= response_div_count:
                    # this_logger.debug("i is {}".format(str(i)))
                    # validate whether response div specified is in response dept field
                    validated_response_div = ensure_response_div_is_in_response_dept_list(response_divs_list[i-1].split("/")[0], response_depts_list)
                    if validated_response_div:
                        icursor_by_div.insertRow((oid, request_id, request_no, initiating_entity, request_date, due_date, date_closed, requested_by, press, request_summary, address_1, address_2, parcel_pin_1, parcel_pin_2, request_clarification, response_divs_list[i-1].split("/")[0], records_provided, payment_required, extension_required, information_request, notes, hyperlink, request_month_int, response_divs_list[i-1].strip()))
                        this_logger.debug("By Div fc: request id {} inserted for response div {}".format(str(request_id), str(response_divs_list[i-1])))
                    else:
                        this_logger.debug("By Div fc: response div {} for request id {} isn't amongst record's response depts".format(str(response_divs_list[i-1]), str(request_id)))
                    i+=1
                del icursor_by_div

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def send_log_email(failure_log_recipients):

    try:
        # new HTML-forcing code for fixed-width font to render log logfile in columns
        fp = open(logfile, 'r')
        msg_html = fp.read()
        fp.close()
        if "ERROR" in str(msg_html):
            subject = str("ERROR - " + str(this_file))
            recipients = failure_log_recipients
            sender = "no-reply@thorntonco.gov"
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['To'] = ';'.join(recipients)
            msg['From'] = sender
            # Create the tags for the HTML version of the message so it has a fixed width font
            prefix_html = '''\
            <html>
            <head></head>
            <body>
              <p style="font-family:'Lucida Console', Monaco, monospace;font-size:12px">
            '''
            suffix_html = '''\
              </p>
            </body>
            </html>
            '''
            # replace spaces with non-breaking spaces (otherwise, multiple spaces are truncated)
            msg_html = msg_html.replace(' ', '&nbsp;')
            # replace new lines with <br> tags and add the HTML tags before and after the message
            msg_html = prefix_html + msg_html.replace('\n', '<br>') + suffix_html

            # # Record the MIME types of both parts - text/plain and text/html.
            #part1 = MIMEText(msgPlain, 'plain')
            part2 = MIMEText(msg_html, 'html')

            # Add both forms of the message
            #msg.attach(part1)
            msg.attach(part2)

            # Connect to exchange and send email
            conn = smtplib.SMTP('casmail')
            conn.ehlo()
            # conn.starttls()
            conn.ehlo()
            conn.sendmail(sender, recipients, msg.as_string())
            conn.close()

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def send_email(recipients, cc_recipients, bcc_recipients, msgHTML, subject):

    try:
        sender = "records-requests@thorntonco.gov"
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['To'] = ';'.join(recipients)
        msg['Cc'] = ';'.join(cc_recipients)
        msg['Bcc'] = ';'.join(bcc_recipients)
        msg['From'] = sender
        prefix_html = '''\
        <html>
        <head></head>
        <body>
          <p style="font-family:'Arial', Monaco, monospace;font-size:13px">
        '''
        suffix_html = '''Technical issues? <a href="mailto:steve.sanford@thorntonco.gov;michael.green@thorntonco.gov">Email us.</a>
          </p>
        </body>
        </html>
        '''
        msg_html = msg_html.replace(' ', '&nbsp;')
        msg_html = prefix_html + msg_html.replace('\n', '<br>') + suffix_html
        part2 = MIMEText(msg_html, 'html')
        msg.attach(part2)
        conn = smtplib.SMTP('xxxx')
        conn.ehlo()
        # conn.starttls()
        conn.ehlo()
        # this_logger.debug("here1")
        conn.sendmail(sender, recipients+cc_recipients+bcc_recipients, msg.as_string())
        conn.close()
        this_logger.debug("email sent")

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

if __name__ == '__main__':

    main()


