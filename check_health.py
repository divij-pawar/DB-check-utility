"""Main file to run"""
import os
import sys
import json
import time
from datetime import datetime
import pyodbc
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd


class ColorPrint:
    def __init__(self, *args, color_code="0"):
        self.args = args
        self.color_code = color_code

    def __str__(self):
        text = " ".join(str(arg) for arg in self.args)
        return f"\033[{self.color_code}m{text}\033[0m"
    
def print_red(*args):
    red_print = ColorPrint(*args, color_code="31")
    print(red_print)

def print_green(*args):
    green_print = ColorPrint(*args, color_code="32")
    print(green_print)
    
def print_yellow(*args):
    yellow_print = ColorPrint(*args, color_code="33")
    print(yellow_print)

class DatabaseConnection():
    def __init__(self) -> None:
        super().__init__()
        self.pg_con = self.pg_connection()
        self.pg_con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
    def db_connection_dst(self, server,username, password):
        """Connect to destination database"""
        try:
            cnxn = pyodbc.connect(
                "DRIVER=ODBC Driver 17 for SQL Server"+
                f";SERVER={server}"+
                f";UID={username}"+
                f";PWD={password}"+
                ";TrustServerCertificate=yes"
            )
            return cnxn

        except Exception as e:
            print_red("[Error] in (utils.util,db_connection_dst) msg: ", str(e))
            sys.exit(1)

    def _forFetchingJson(self, con, query,one=False):
        try :
            cur = con.cursor()
            cur.execute(query)
            r = [dict((cur.description[i][0].lower(), value) \
                    for i, value in enumerate(row)) for row in cur.fetchall()]
            cur.close()
            return (r[0] if r else None) if one else r
        except Exception as e :
            print(query)
            print("[Error] in (utils.util,_forFetchingJson) msg: ",str(e))

    def _forFetchingJsonPG(self, query,one=False):
        try :
            cur =self.pg_con.cursor()
            cur.execute(query)
            r = [dict((cur.description[i][0].lower(), value) \
                    for i, value in enumerate(row)) for row in cur.fetchall()]
            cur.close()
            return (r[0] if r else None) if one else r
        except Exception as e :
            print(query)
            print("[Error] in (utils.util,_forFetchingJson) msg: ",str(e))
                        
    def pg_connection(self):
        """Postgres connection to database"""
        try:
            cnxn = psycopg2.connect(
                host= "127.0.0.1",
                port = 5432,
                database =  "RMS",
                user="postgres",
                password= "Admin@4321")
            return cnxn
        except Exception as e:
            print_red("[Error] in (utils.util,pg_connection) msg: ", str(e))
            sys.exit(1)
            

    def _executeQuery(self,con, query) -> None:
        """Execute a query in Destination Database without return value"""
        try:
            cur = con.cursor()
            cur.execute(query)
            con.commit()
            cur.close()
        except pyodbc.Error as ex:
                sqlstate = ex.args[0]
                if not sqlstate == '23000':
                    print_red(query)
                    print_red("\n[Error] in (utils.util,_executeQuery) msg: ", str(ex))
    
    def fetch_id(self, value, col_name, tb):
        """ Fetch ID from Destination database"""
        try:
            cur = self.pg_con.cursor()
            query = f"""SELECT id FROM "{tb}" WHERE {col_name} = '{value}'"""
            cur.execute(query)

            result = cur.fetchone()
            if result:
                return result[0]
            cur.close()
        except Exception as e:
            print_red(query)
            print_red("\n[Error] in (utils.util,fetch_id) msg: ", str(e))

class Main(DatabaseConnection):
    def __init__(self) -> None:
        super().__init__()

        # Get date as input
        try:
            self.table_date = input("Enter the date in format YYYYMMDD: ")
            # self.table_date = "20231103"
        except:
            print_red("Please enter the date for which you want to check data")
            sys.exit(1)

        # Check if the date is a valid date
        try:
            self.t = datetime.strptime(self.table_date, '%Y%m%d')
        except ValueError:
            print_red("Please enter the date in format YYYYMMDD: ")
            sys.exit(1)

        self.ms_con_nse = self.db_connection_dst("192.168.15.104","sa","Admin@4321")
        self.ms_con_mcx = self.db_connection_dst("192.168.15.105","sa","Admin@4321")

        todays_date = datetime.today().strftime('%Y%m%d')
        if self.table_date == todays_date:
            self.time_parameter = 'LIVE'
            self.is_live = True

        else:
            self.time_parameter = 'RECORDS'
            self.is_live = False
        self.menu()

    def menu(self):
        os.system('clear')
        print_yellow(f"Date received: {self.t.date()}.\t\t\t \033[1m *{self.time_parameter}* \033[0m")
        option = input("########### RMS Database Check Utility ###########\n1. Match Count\n2. Match Quantity\n3. Match MTM\n4. Match MTM UserID wise\n5. Exit \nEnter Option: ")
        if option == '1':
            self.match_count()
        elif option == '2':
            self.match_quantity()
        elif option == '3':
            self.match_mtm()
        elif option == '4':
            self.match_mtm_userid()
        elif option =='5' or option =='exit' or option =='0':
            sys.exit(0)
        else:
            print_red("Enter a valid option. Exiting.")
            self.menu()
    
    def match_count(self):
        print_yellow("_" * (((os.get_terminal_size().columns)-len("Matching Count"))//2) + "Matching Count"+"_" * (((os.get_terminal_size().columns)-len("Matching Count"))//2))
        ms_dict = {}
        for i in range (1,10):
            query = f"""
                    select COUNT(*)s{i} from NSES{i}.DBO.tbTradeBook WHERE DATETIME LIKE '{self.table_date}%'
                    """
            data = self._forFetchingJson(self.ms_con_nse,query)
            ms_dict.update(data[0])
        for i in range (1,5):
            query = f"""
                    select COUNT(*)m{i} from  MCXS{i}.DBO.tbTradeBook WHERE DATETIME LIKE '{self.table_date}%'
                    """
            data = self._forFetchingJson(self.ms_con_mcx,query)
            ms_dict.update(data[0])

        query = f"select sender,count(*) from {'trades' if self.is_live else 'tradebook'} where date='{self.table_date}'  group by sender order by sender"
        data = self._forFetchingJsonPG(query)
        # Initialize an empty dictionary
        pg_dict = {}
        for item in data:
            sender = item['sender']
            count = item['count']
            pg_dict[sender] = count
        print(f"{'Sender':<10} {'MS Count':<10} {'PG Count':<10} {'Difference':<10}")
        print_yellow("_"*45)
        for key in ms_dict:
            difference = ms_dict[key] - pg_dict.get(key,0)
            output = f"{key.upper():<10} {ms_dict[key]:<10} { pg_dict.get(key,0):<10} {difference:<10} "
            if difference == 0:
                print_green(output)
            else:
                print_red(output)
        input("Press enter to continue...")
        os.system('clear')
        print("Finished Executing")
        self.menu()

    def match_quantity(self):
        print_yellow("_" * (((os.get_terminal_size().columns)-len("Matching Quantity"))//2) + "Matching Quantity"+"_" * (((os.get_terminal_size().columns)-len("Matching Quantity"))//2))


        #MS NSE ########################################################################
        ms_dict = {}
        for i in range (1,10):
            query = f"""
                    select EXCHANGE,sender,
                    sum(case when  buysellflag='buy' then (TradeQty) else 0 end )buyqty,
                    sum(case when  buysellflag='sell' then (TradeQty)  else 0 end)sellqty
                    from NSES{i}.DBO.tbTradeBook WHERE DATETIME LIKE '{self.table_date}%'    
                    group by EXCHANGE,sender
                    order by sender,EXCHANGE
                    """
            data = self._forFetchingJson(self.ms_con_nse,query)
            for item in data:
                sender = item['sender']
                exchange = item['exchange']
                buyqty = item['buyqty']
                sellqty = item['sellqty'] 
                # Create the inner dictionary for the exchange data
                exchange_data = {'buyqty': buyqty, 'sellqty': sellqty}
                if sender not in ms_dict:
                    ms_dict[sender] = {}
                if exchange not in ms_dict[sender]:
                    ms_dict[sender][exchange] = exchange_data
                else:
                    for paramter in exchange_data:
                        ms_dict[sender][exchange][paramter] += exchange_data[paramter]
        #MS MCX ########################################################################
        for i in range (1,5):
            query = f"""
                    select EXCHANGE, sender,
                    sum(case when  buysellflag='buy' then (TradeQty) else 0 end )buyqty,
                    sum(case when  buysellflag='sell' then (TradeQty)  else 0 end)sellqty
                    from MCXS{i}.DBO.tbTradeBook WHERE DATETIME LIKE '{self.table_date}%'    
                    group by EXCHANGE,sender
                    order by sender,EXCHANGE
                    """
            data = self._forFetchingJson(self.ms_con_mcx,query)
            for item in data:
                sender = f'm{i}'
                exchange = item['exchange'].split(':')[0]
                buyqty = item['buyqty']
                sellqty = item['sellqty']
                # Create the inner dictionary for the exchange data
                exchange_data = {'buyqty': buyqty, 'sellqty': sellqty}
                if sender not in ms_dict:
                    ms_dict[sender] = {}
                if exchange not in ms_dict[sender]:
                    ms_dict[sender][exchange] = exchange_data
                else:
                    for paramter in exchange_data:
                        ms_dict[sender][exchange][paramter] += exchange_data[paramter]

        # Postgres ########################################################################
        pg_dict = {}
        query = f"""
                select EXCHANGE,sender,
                sum(case when  buysellflag='1' then (TradeQty) else 0 end )buyqty,
                sum(case when  buysellflag='2' then (TradeQty)  else 0 end)sellqty
                from {'trades' if self.is_live else 'tradebook'} where date='{self.table_date}'  
                group by EXCHANGE,sender
                order by sender,EXCHANGE"""
        data = self._forFetchingJsonPG(query)
        # Iterate through the list of dictionaries and create the dictionary of dictionaries
        for item in data:
            sender = item['sender']
            exchange = item['exchange']
            buyqty = item['buyqty']
            sellqty = item['sellqty']
            
            # Create the inner dictionary for the exchange data
            exchange_data = {'buyqty': buyqty, 'sellqty': sellqty}
            
            if sender not in pg_dict:
                pg_dict[sender] = {}
            pg_dict[sender][exchange] = exchange_data

        # Output Rendering ########################################################################
        line = "-" * os.get_terminal_size().columns
        print(f"{'Sender':<10} {'Parameter':<10} {'Exchange':<10} {'MS':<20} {'PG':<20} {'Difference':<20}")
        print_yellow("_"*90)

        # Match the two systems to find difference
        senders = ['s1','s2','s3','s4','s5','s6','s7','s8','s9','m1','m2','m3','m4']
        for sender in senders:
            # if sender.upper()=='S9':
            #     input("Press enter to continue...")
            #     os.system('clear')
            #     print(f"{'Sender':<10} {'Parameter':<10} {'Exchange':<10} {'MS':<20} {'PG':<20} {'Difference':<20}")
                
            if not pg_dict.get(sender):
                print_red(f"{sender.upper()} not present in PG")
                continue
            if not ms_dict.get(sender):
                print_red(f"{sender.upper()} not present in MS")
                continue

            for exchange in ms_dict.get(sender):
                if not pg_dict.get(sender).get(exchange):
                    print_red(f"{sender.upper()} sender of PG does not contain exchange {exchange}")
                    continue

                buyqty_difference = ms_dict.get(sender).get(exchange).get('buyqty',0) - pg_dict.get(sender).get(exchange).get('buyqty',0)
                output = f"{sender.upper():<10} {'BuyQty':<10} {exchange.upper():<10} {ms_dict.get(sender).get(exchange).get('buyqty',0):<20} {pg_dict.get(sender).get(exchange).get('buyqty',0):<20} {buyqty_difference:<20}"
                if buyqty_difference == 0:
                    print_green(output)
                else:
                    print_red(output)
                sellqty_difference = ms_dict.get(sender).get(exchange).get('sellqty',0) - pg_dict.get(sender).get(exchange).get('sellqty',0)
                output = f"{sender.upper():<10} {'SellQty':<10} {exchange.upper():<10} {ms_dict.get(sender).get(exchange).get('sellqty',0):<20} {pg_dict.get(sender).get(exchange).get('sellqty',0):<20} {sellqty_difference:<20}"
                if sellqty_difference == 0:
                    print_green(output)
                else:
                    print_red(output)
                print()


                
        input("Press enter to continue...")
        os.system('clear')
        print("Finished Executing")
        self.menu()

    def match_mtm(self):
        print_yellow("_" * (((os.get_terminal_size().columns)-len("Matching MTM"))//2) + "Matching MTM"+"_" * (((os.get_terminal_size().columns)-len("Matching MTM"))//2))

        #MS NSE ########################################################################
        ms_dict = {}
        for exchange in ['NSEFO','NSEIFSC','SGXFO']:
            query = f"""
                    select SUM(BFQTY)BFQTY,sum(BUYQTY)BUYQTY,sum(SELLQTY)SELLQTY,sum(NETQTY)NETQTY,sum(CFQTY)CFQTY,SUM(BFAMT)BFAMT,
                    sum(BUYAMT)BUYAMT,sum(SELLAMT)SELLAMT,sum(CFAMT)CFAMT,sum({'LIVEMTM' if self.is_live else 'MANUALMTM'})grossMTM,
                    sum(TODAYBROKAMT)Brokerage,sum({'NETLIVEMTMT' if self.is_live else 'MANUALNETMTM'})NETMTM
                    from RMS.dbo.{'netposition' if self.is_live else 'netpositionrec'}
                    where DATETIME='{self.table_date}'
                    and exchange = '{exchange}' { '' if self.is_live else 'and MANUALNETMTM<>0'}
                    """
            data = self._forFetchingJson(self.ms_con_nse,query)
            ms_dict[exchange] = data[0]

        #MS MCX ########################################################################
        for exchange in ['MCXFIN','CME','DGCX']:
            query = f"""
                    select sum(OPENQTY)BFQTY,sum(BUYQTY)BUYQTY,sum(SELLQTY)SELLQTY,sum(NETQTY)NETQTY,sum(CLOSEQTY)CFQTY,
                    sum(OPENAMT)BFAMT,sum(BUYAMT)BUYAMT,sum(SELLAMT)SELLAMT,sum(CLOSEAMT)CFAMT,sum({'LIVEMTM' if self.is_live else 'SETTLEMTM'})grossMTM,sum(TODAYBROK)Brokerage,
                    sum({'NETMTM' if self.is_live else 'SETTLETODAYSMTM'})NETMTM
                    from MCXRMS.dbo.{'TradeSummary'if self.is_live else 'TradeSummaryRec'} WHERE  DATETIME='{self.table_date}'
                    and exchange = '{exchange}' {'' if self.is_live else 'and SETTLETODAYSMTM<>0'}
                    """
            source_data = self._forFetchingJson(self.ms_con_mcx,query)
            if exchange == 'MCXFIN':
                exchange = 'MCX'
            ms_dict[exchange] = source_data[0]


        # Postgres and output rendering ########################################################################
        line = "_" * os.get_terminal_size().columns
        pg_dict = {}
        parameter_match_flag = True
        for exchange in ['NSEFO','NSEIFSC','MCX','CME',"DGCX"]:
            query = f"""
                    select SUM(BFQTY)BFQTY,sum(BUYQTY)BUYQTY,sum(SELLQTY)SELLQTY,sum(NETQTY)NETQTY,sum(CFQTY)CFQTY,
                    SUM(BFAMT)BFAMT,sum(BUYAMT)BUYAMT,sum(SELLAMT)SELLAMT,sum(CFAMT)CFAMT,sum(grossmtm)grossMTM,
                    sum(charges)Brokerage,sum(netmtm)NETMTM from {'netposition' if self.is_live else 'netposition_rec' }
                    where  exchange='{exchange}'  and date='{self.table_date}'"""
            data = self._forFetchingJsonPG(query)
            pg_dict[exchange] = data[0]
            pg_dict[exchange] = {key: value for key, value in pg_dict[exchange].items() if value is not None}
            ms_dict[exchange] = {key: value for key, value in ms_dict[exchange].items() if value is not None}
            continue_flag = False
            if not ms_dict.get(exchange):
                print_red(f'{exchange} not present in MS')
                time.sleep(1)
                continue_flag =True
            if not pg_dict.get(exchange):
                print_red(f'{exchange} not present in PG')
                time.sleep(1)
                continue_flag =True
            if continue_flag:
                continue
            print_yellow(" " * (((os.get_terminal_size().columns)-len(exchange))//2) +"\033[1m"+ exchange +"\033[0m")
            print(line)
            print(f"{'Paramter':<10} {'MS':<20} {'PG':<20} {'Difference (-1 <= diff <= 1) is ok'}")
            print(line)

            to_match = ['bfqty','buyqty','sellqty','netqty','cfqty','bfamt','buyamt','sellamt','cfamt','grossmtm','brokerage','netmtm']
            for paramter in to_match:
                difference = ms_dict.get(exchange,0).get(paramter,0) - pg_dict.get(exchange,0).get(paramter,0) 
                if difference > -1 and difference < 1:
                    output = f"{paramter:<10}  {ms_dict.get(exchange).get(paramter,0):<20} {pg_dict.get(exchange).get(paramter,0):<20} {difference}"
                    print_green(output)
                else:
                    output = f"{paramter:<10} {ms_dict.get(exchange).get(paramter,0):<20} {pg_dict.get(exchange).get(paramter,0):<20} {difference}"
                    print_red(output)
                    parameter_match_flag = False

            print(line)
            if parameter_match_flag:
                last_line = f"\033[1m {exchange} completely matched \033[0m"
                print_green(" " * (((os.get_terminal_size().columns)-len(last_line))//2) + last_line)
            else:
                last_line = f"\033[1m {exchange} has unmatched paramters \033[0m"
                print_red(" " * (((os.get_terminal_size().columns)-len(last_line))//2) + last_line)

                parameter_match_flag = True
            print(line)
            input_option = input("Press enter to goto to next page...\n0 to goto menu ")
            if input_option == '0':
                self.menu()
            os.system('clear')
        print("Finished Executing")
        self.menu()

    def match_mtm_userid(self):
        print_yellow("_" * (((os.get_terminal_size().columns)-len("Matching MTM UserID wise"))//2) + "Matching MTM UserID wise"+"_" * (((os.get_terminal_size().columns)-len("Matching MTM UserID wise"))//2))


        #MS NSE ########################################################################
        ms_dict = {}
        for exchange in ['NSEFO','NSEIFSC','NSEFO']:
            query = f"""
                    select userid,SUM(BFQTY)BFQTY,sum(BUYQTY)BUYQTY,sum(SELLQTY)SELLQTY,sum(NETQTY)NETQTY,sum(CFQTY)CFQTY,SUM(BFAMT)BFAMT,
                    sum(BUYAMT)BUYAMT,sum(SELLAMT)SELLAMT,sum(CFAMT)CFAMT,sum({'LIVEMTM' if self.is_live else 'MANUALMTM'})grossMTM,
                    sum(TODAYBROKAMT)Brokerage,sum({'NETLIVEMTMT' if self.is_live else 'MANUALNETMTM'})NETMTM
                    from RMS.dbo.{'netposition' if self.is_live else 'netpositionrec'}
                    where DATETIME='{self.table_date}' and exchange = '{exchange}' {''  if self.is_live else 'and MANUALNETMTM<>0'}
                    group by userid order by userid
                    """
            data = self._forFetchingJson(self.ms_con_nse,query)
            userdict = {}
            for row in data:
                userdict[row['userid']] = row
            ms_dict[exchange] = userdict

        #MS MCX ########################################################################
        for exchange in ['MCXFIN','CME','DGCX']:
            query = f"""
                    select userid,sum(OPENQTY)BFQTY,sum(BUYQTY)BUYQTY,sum(SELLQTY)SELLQTY,sum(NETQTY)NETQTY,sum(CLOSEQTY)CFQTY,
                    sum(OPENAMT)BFAMT,sum(BUYAMT)BUYAMT,sum(SELLAMT)SELLAMT,sum(CLOSEAMT)CFAMT,sum({'LIVEMTM' if self.is_live else 'SETTLEMTM'})grossMTM,sum(TODAYBROK)Brokerage,
                    sum({'NETMTM' if self.is_live else 'SETTLETODAYSMTM'})NETMTM
                    from MCXRMS.dbo.{'TradeSummary' if self.is_live else 'TradeSummaryRec'} WHERE  DATETIME='{self.table_date}'
                    and exchange = '{exchange}' {'' if self.is_live else 'and SETTLETODAYSMTM <> 0'}
                    group by userid order by userid
                    """
            data = self._forFetchingJson(self.ms_con_mcx,query)
            if exchange == 'MCXFIN':
                exchange = 'MCX'
            userdict = {}
            for row in data:
                userdict[row['userid']] = row
            ms_dict[exchange] = userdict


        # Postgres and matching ########################################################################
        pg_dict = {}
        for exchange in ['NSEFO','NSEIFSC','MCX','CME',"DGCX"]:
            query = f"""
                    select userid,SUM(BFQTY)BFQTY,sum(BUYQTY)BUYQTY,sum(SELLQTY)SELLQTY,sum(NETQTY)NETQTY,sum(CFQTY)CFQTY,
                    SUM(BFAMT)BFAMT,sum(BUYAMT)BUYAMT,sum(SELLAMT)SELLAMT,sum(CFAMT)CFAMT,sum(grossmtm)grossMTM,
                    sum(charges)Brokerage,sum(netmtm)NETMTM from {'netposition' if self.is_live else 'netposition_rec'}
                    where  exchange='{exchange}'  and date='{self.table_date}'  group by userid order by userid
                    """
            data = self._forFetchingJsonPG(query)
            userdict = {}
            for row in data:
                userdict[row['userid']] = row
            pg_dict[exchange] = userdict

            # Output Rendering ########################################################################
            # Set variables
            red = "\033[91m"
            green = "\033[92m"
            end_color = "\033[0m"
            line = "_" * os.get_terminal_size().columns
            parameter_match_flag = True # To check if all paramters are matched
            # Add the columns from to_match
            to_match = ['bfqty','buyqty','sellqty','netqty','cfqty','bfamt','buyamt','sellamt','cfamt','grossmtm','brokerage','netmtm']
            column_line = f"{'DB':-<5}-{'UserID':-<10}"
            for col in to_match:
                column_line += f"{col:-<15}"


            #...................................................................OUTPUT STARTS HERE..............................................................
            print_yellow(" " * (((os.get_terminal_size().columns)-len(exchange))//2) +"\033[1m"+ exchange +"\033[0m")
            print(line)
            print()
            # Iterate over all userids in MS
            for userid in ms_dict[exchange]:
                print(column_line)
                continue_flag = False
                if not pg_dict.get(exchange).get(userid):
                    print_red(f"\t{userid} present in MS but not present in PG {exchange}")
                    continue_flag = True
                    time.sleep(0.1)
                if continue_flag:
                    continue
                
                # Print Scanlines
                print(f"{'MS':<5} {userid:<10} ", end="")
                output=""
                for paramter in to_match:
                    output += f"{round(ms_dict.get(exchange).get(userid).get(paramter),2):<15}"
                print(output)
                
                print(f"{'PG':<5} {userid:<10} ", end="")
                output=""
                for paramter in to_match:
                    output += f"{round(pg_dict.get(exchange).get(userid).get(paramter),2):<15}"
                print(output)
                
                print(f"{'Diff':<5} {userid:<10} ", end="")
                output=""
                for paramter in to_match:
                    difference = abs(ms_dict.get(exchange).get(userid).get(paramter)) - abs(pg_dict.get(exchange).get(userid).get(paramter))
                    difference = round(difference, 2)
                    if difference > -1 and difference < 1:
                        formatted_difference = f"{green}{difference:<15}{end_color}"
                    else:
                        formatted_difference = f"{red}{difference:<15}{end_color}"
                        parameter_match_flag = False
                    output += formatted_difference
                print(output)
            print(line)
            if parameter_match_flag:
                last_line = f"\033[1m {exchange} completely matched \033[0m"
                print_green(" " * (((os.get_terminal_size().columns)-len(last_line))//2) + last_line)
            else:
                last_line = f"\033[1m {exchange} has unmatched paramters \033[0m"
                print_red(" " * (((os.get_terminal_size().columns)-len(last_line))//2) + last_line)

                parameter_match_flag = True
            print(line)
            input_option = input("Press enter to goto to next page...\n0 to goto menu ")
            if input_option == '0':
                self.menu()
            os.system('clear')
        print("Finished Executing")
        self.menu()

Main()