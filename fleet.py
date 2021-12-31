import datetime
import credentials
import matplotlib.pyplot as plt
import numpy as np
from mysql.connector import (connection)
from googletrans import Translator

def getConnectionCursor():
    cnx = connection.MySQLConnection(user=credentials.user, 
                                    password=credentials.password,
                                    host=credentials.host,
                                    database=credentials.database)
    return (cnx, cnx.cursor(buffered=True))

def getTopCarsByModel(limit):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Total_Number_of_Leases_by_Car_Model', [limit])
    cars = {}
    for res in cursor.stored_results():
        for (car_model, quantity, co2) in res:
            cars[car_model] = (quantity, co2)

    cursor.close()
    cnx.close()

    return cars

def getTopCitiesByTotalNumberOfLeases(limit):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Total_Number_of_Leases_by_City_id', [limit])
    cities = {}
    for res in cursor.stored_results():
        for (name, city_id, leases) in res:
            cities[city_id] = (name, leases)

    cursor.close()
    cnx.close()

    return cities

def getTopCarsByModelInCity(city_id ,limit):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Total_Number_of_Leases_by_Car_Model_in_City', [city_id, limit])
    cars = {}
    for res in cursor.stored_results():
        for (car_model, quantity, consumption, co2) in res:
            co2 = 0 if co2 == None else int(co2)
            cars[car_model] = (quantity, consumption, co2)

    cursor.close()
    cnx.close()

    return cars

def getMonthDistributionForLeaseStart(query):
    cnx, cursor = getConnectionCursor()
    cursor.execute(query)
    datas = {}

    for (h_day,) in cursor:
        month = datetime.datetime.now()
        try:
            month = datetime.datetime(h_day.year, h_day.month, 1)
        except:
            continue
        if month.year < 2015 or month.year >= 2022:
            continue
        datas[month] = datas.get(month, 0) + 1

    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month

def getTopOptionsListByCityAndCar(city_id, car_model, limit):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Options_list_by_city_and_car_model_leasing', [city_id, car_model])
    options_map = {}
    c = 0
    for res in cursor.stored_results():
        for (id, options) in res:
            if options == None or options == "":
                c += 1
                continue
            options_list = options.split("; ")
            for option in options_list:
                if option.lower() == "alennus" or option.lower() == "toimituskulu" or option.lower() == "toimituskulut":
                    continue
                options_map[option] = options_map.get(option, 0) + 1

    cursor.close()
    cnx.close()

    options_list_top = dict(sorted(options_map.items(), key=lambda item: item[1], reverse=True)[:limit])
    options_list_top["No options"] = c

    return options_list_top

def getTopOptionsListByCar(car_model, limit):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Options_list_by_car_model_leasing', [car_model])
    options_map = {}
    c = 0
    for res in cursor.stored_results():
        for (id, options) in res:
            if options == None or options == "":
                c += 1
                continue
            options_list = options.split("; ")
            for option in options_list:
                if option.lower() == "alennus" or option.lower() == "toimituskulu" or option.lower() == "toimituskulut":
                    continue
                options_map[option] = options_map.get(option, 0) + 1

    cursor.close()
    cnx.close()

    options_list_top = dict(sorted(options_map.items(), key=lambda item: item[1], reverse=True)[:limit])
    options_list_top["No options"] = c

    return options_list_top

def getPurchasesByCategoryCityAndCarDistributionByMonth(costGroup, city_id, car_model):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Purchases_By_Category_City_and_Car', [costGroup, city_id, car_model])
    datas = {}

    for res in cursor.stored_results():
        for row in res:
            h_day = row[2]
            month = datetime.datetime.now()
            try:
                month = datetime.datetime(h_day.year, h_day.month, 1)
            except:
                continue
            if month.year < 2015 or month > datetime.datetime(2021, 10, 1):
                continue

            datas[month] = datas.get(month, 0) + 1
        
    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month  

def getPurchasesByCategoryAndCarDistributionByMonth(costGroup, car_model):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Purchases_By_Category_and_Car', [costGroup, car_model])
    datas = {}

    for res in cursor.stored_results():
        for row in res:
            h_day = row[2]
            month = datetime.datetime.now()
            try:
                month = datetime.datetime(h_day.year, h_day.month, 1)
            except:
                continue
            if month.year < 2015 or month > datetime.datetime(2021, 10, 1):
                continue

            datas[month] = datas.get(month, 0) + 1
        
    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month  

def getPurchasesByCategoryCityAndFuelDistributionByMonth(costGroup, city_id, fuel):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Purchases_By_Category_City_and_Fuel', [costGroup, city_id, fuel])
    datas = {}

    for res in cursor.stored_results():
        for row in res:
            h_day = row[2]
            month = datetime.datetime.now()
            try:
                month = datetime.datetime(h_day.year, h_day.month, 1)
            except:
                continue

            datas[month] = datas.get(month, 0) + 1
        
    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month  

def getTotalCostsByCategoryCityAndFuelDistributionByMonth(costGroup, city_id, fuel):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Purchases_By_Category_City_and_Fuel_all', [costGroup, city_id, fuel])
    datas = {}

    for res in cursor.stored_results():
        for row in res:
            h_day = row[2]
            total_amount = 0
            month = datetime.datetime.now()
            try:
                month = datetime.datetime(h_day.year, h_day.month, 1)
                total_amount = float(row[4])
            except:
                continue
            if month.year < 2015 or month > datetime.datetime(2021, 10, 1):
                continue

            datas[month] = datas.get(month, 0) + total_amount
        
    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month  

def getTotalCostsByCategoryAndFuelDistributionByMonth(costGroup, fuel):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Purchases_By_Category_and_Fuel_all', [costGroup, fuel])
    datas = {}

    for res in cursor.stored_results():
        for row in res:
            h_day = row[2]
            total_amount = 0
            month = datetime.datetime.now()
            try:
                month = datetime.datetime(h_day.year, h_day.month, 1)
                total_amount = float(row[4])
            except:
                continue
            if month.year < 2015 or month > datetime.datetime(2021, 10, 1):
                continue

            datas[month] = datas.get(month, 0) + total_amount
        
    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month  

def getTotalNumberOfLeasesByCategoryAndFuelDistributionByMonth(costGroup, fuel):
    cnx, cursor = getConnectionCursor()
    cursor.callproc('Get_Purchases_By_Category_and_Fuel_all', [costGroup, fuel])
    datas = {}

    for res in cursor.stored_results():
        for row in res:
            h_day = row[2]
            month = datetime.datetime.now()
            try:
                month = datetime.datetime(h_day.year, h_day.month, 1)
            except:
                continue
            if month.year < 2015 or month > datetime.datetime(2021, 10, 1):
                continue

            datas[month] = datas.get(month, 0) + 1
        
    datas_sort_by_month = dict(sorted(datas.items(), key=lambda item: item[0], reverse=True))
    
    cursor.close()
    cnx.close()
    
    return datas_sort_by_month  


# main program

cost_groups = {
    "Lease" : "100,101,103,110", 
    "Rent" : "102,104", 
    "Maintain" : "112,114,115,300,500,501,700,810,900,910,993", 
    "Taxes" : "200,301,302,401,402,991,992", 
    "Internal" : "120,310,311,400,500,501,600,800,820,990", 
    "Finance" : "111,116,911,901,950" }

all_costs = ",".join(cost_groups.values())
other_costs = ",".join(list(cost_groups.values())[2:])
income_costs = "901,910,911,950,990,991,992,993"

fuel_groups = {
        "Petrol/Diesel" : "D,95E,98E",
        "Hybrid" : "ED,EB,M",
        "Electro" : "E" }

# local cars distribution by cities with options
'''
cities = getTopCitiesByTotalNumberOfLeases(5)

for (city_id, val) in cities.items():
    title = "Average number of active leases in a month in {}".format(val[0])
    plot = plt.figure(title)
    ax = plot.add_subplot(111)
    ax.yaxis.tick_right()
    ax.title.set_text(title)
    cars = getTopCarsByModelInCity(city_id, 10)

    for (car, val) in cars.items():
        datas_sort_by_month = getPurchasesByCategoryCityAndCarDistributionByMonth(cost_groups["Lease"], city_id, car)
        top_options = getTopOptionsListByCityAndCar(city_id, car, 3)
        translator = Translator()
        options = "; ".join("{} ({})".format(fin_op[0], fin_op[1]) for fin_op in top_options.items())

        if len(datas_sort_by_month) == 0:
            continue
        x, y = zip(*datas_sort_by_month.items())
        label_string = f"{car} - Total ({val[0]}), CO2 ({val[2]}) : {options}"
        plt.plot(x, y, marker='o', markersize=2, label=label_string)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.25, box.width, box.height * 0.75])
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.07), fontsize=8)
plt.show()
'''

# global cars distribution with top options
'''
plot = plt.figure("Average number of leases in a Month")

ax = plot.add_subplot(111)
ax.yaxis.tick_right()
ax.title.set_text("Average number of active leases by month for top 10 car models and its top 5 options")

cars = getTopCarsByModel(10)

for (car, val) in cars.items():
    datas_sort_by_month = getPurchasesByCategoryAndCarDistributionByMonth(cost_groups["Lease"], car)
    top_options = getTopOptionsListByCar(car, 5)
    translator = Translator()
    options = "; ".join("{} ({})".format(translator.translate(fin_op[0]).text, fin_op[1]) for fin_op in top_options.items())

    if len(datas_sort_by_month) == 0:
        continue
    x, y = zip(*datas_sort_by_month.items())
    label_string = f"{car} - Total ({val[0]}), Avg CO2 ({val[1]}) : {options}"
    plt.plot(x, y, marker='o', markersize=2, label=label_string)
    
box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.25, box.width, box.height * 0.75])

plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.07), fontsize=8)
plt.show()
'''

# local leases distribution by fuel
'''
cities = getTopCitiesByTotalNumberOfLeases(5)

for (city_id, val) in cities.items():
    plot = plt.figure("Average number of leases in a Month in {} - total number: {}".format(val[0], int(val[1])))
    ax = plot.add_subplot(111)
    ax.yaxis.tick_right()

    for (fuel_name,fuel_code) in fuel_groups.items():
        datas_sort_by_month = getPurchasesByCategoryCityAndFuelDistributionByMonth(cost_groups["Lease"], city_id, fuel_code)
        if len(datas_sort_by_month) == 0:
                continue
        x, y = zip(*datas_sort_by_month.items())
        plt.plot(x, y, marker='o', markersize=2, label=fuel_name)

    plt.legend(bbox_to_anchor=(-0.1, 1.1), loc='upper left', fontsize=8)

plt.show()
'''


# local costs trends by category, city and fuel
'''
cities = getTopCitiesByTotalNumberOfLeases(3)

colors = ['blue','red','green']
    
for (city_id, val) in cities.items():
    for (fuel_name,fuel_code) in fuel_groups.items():
        plot = plt.figure("Monthly total costs in {} for {} vehicles".format(val[0], fuel_name))
        ax2 = plot.add_subplot(111)
        ax2.yaxis.tick_right()
        c = 0
        for (name, cost_type) in {"all_costs": all_costs, "other_costs": other_costs, "income_costs": income_costs}.items():
            #ax2 = ax.twinx()
            #ax2.yaxis.tick_right()
            datas_sort_by_month = getTotalCostsByCategoryCityAndFuelDistributionByMonth(cost_type, city_id, fuel_code)
            if len(datas_sort_by_month) == 0:
                continue
            x, y = zip(*datas_sort_by_month.items())
            
            # Points on Y-axis
            y_data = np.array(y) 
            # numerical axis x
            s_date = 2015*12
            numeric_x = []
            for i in range(len(x)):
                date = x[i]
                numeric_x.append(date.year*12+date.month-s_date)
                if y_data[i] <= 0 and not i == 0 and not i == len(x)-1:
                    y_data[i] = (y_data[i-1]+y_data[i+1])/2
                elif y_data[i] <= 0 and i == 0:
                    y_data[i] = y_data[i+1]
                elif y_data[i] <= 0 and i == len(x)-1:
                    y_data[i] = y_data[i-1]
            
            # Points on X-axis
            x_data = np.array(numeric_x) 
            
            plt.plot(x_data, y_data, marker='o', markersize=2, label=name, color=colors[c])
            #ax2.tick_params(axis='y', color=colors[c])
            #ax2.spines["right"].set_position(("axes", 1+0.05*c))
            #ax2.yaxis.label.set_color(colors[c])

            #trendline
            ylog_data = np.log(y_data)
            curve_fit = np.polyfit(x_data, ylog_data, 1)
            res_y = np.exp(curve_fit[0]*x_data) * np.exp(curve_fit[1])
            plt.plot(x_data,res_y, color=colors[c], label="y = exp({:.5f}*x)*exp({:.2f})".format(curve_fit[0], curve_fit[1]))

            c += 1

        plt.legend(bbox_to_anchor=(-0.1, 1.1), loc='upper left', fontsize=8)

plt.show()
'''

# global costs trends by fuel per car
'''
colors = ['blue','red','green']
    
for (fuel_name,fuel_code) in fuel_groups.items():
    plot = plt.figure("Costs trends for {} vehicles per single car".format(fuel_name))
    ax2 = plot.add_subplot(111)
    ax2.yaxis.tick_right()
    ax2.title.set_text("Costs trends for {} vehicles per single car".format(fuel_name))
    c = 0

    query = f"select Purchases.vehicle_id, Purchases.h_day,
                total_amount, Purchases.cost_group
                from FMX2018_Tbl_ArchiveCost as Purchases
                left join FMX2018_Tbl_Vehicle on Purchases.vehicle_id = FMX2018_Tbl_Vehicle.id
                left join FMX2018_Tbl_CarModel on FMX2018_Tbl_Vehicle.car_id = FMX2018_Tbl_CarModel.id
                where 
                FIND_IN_SET(FMX2018_Tbl_CarModel.fuel, '{fuel_code}')"

    cnx, cursor = getConnectionCursor()
    cursor.execute(query)
    all_data = {}
    other_data = {}
    income_data = {}

    for row in cursor:
        h_day = row[1]
        cost_gr = None
        total_amount = 0
        month = datetime.datetime.now()
        try:
            month = datetime.datetime(h_day.year, h_day.month, 1)
            total_amount = float(row[2])
            cost_gr = str(row[3])
        except:
            continue
        if month <= datetime.datetime(2015, 1, 1) or month > datetime.datetime(2021, 10, 1) or cost_gr == None:
            continue

        all_data[month] = all_data.get(month, 0) + total_amount
        if (cost_gr in other_costs):
            other_data[month] = other_data.get(month, 0) + total_amount
        if (cost_gr in income_costs):
            income_data[month] = income_data.get(month, 0) + total_amount

    all_data = dict(sorted(all_data.items(), key=lambda item: item[0], reverse=True))
    other_data = dict(sorted(other_data.items(), key=lambda item: item[0], reverse=True))
    income_data = dict(sorted(income_data.items(), key=lambda item: item[0], reverse=True))

    cursor.close()
    cnx.close()

    number_of_leases = getTotalNumberOfLeasesByCategoryAndFuelDistributionByMonth(cost_groups["Lease"], fuel_code)\

    for (key, val) in all_data.items():
        if (number_of_leases.get(key, 0) != 0):
            if (all_data.get(key, 0) != 0):
                all_data[key] = all_data[key]/number_of_leases[key]
            if (other_data.get(key, 0) != 0):
                other_data[key] = other_data[key]/number_of_leases[key]
            if (income_data.get(key, 0) != 0):
                income_data[key] = income_data[key]/number_of_leases[key]

    for (name, data) in {"All costs: 100..993": all_data, "Operating costs: 112..993": other_data, "Income costs: 900..993": income_data}.items():
        x, y = zip(*data.items())
        
        # Points on Y-axis
        y_data = np.array(y) 

        # AxisX DateTime to Numeric
        s_date = 2015*12
        numeric_x = []
        for i in range(len(x)):
            date = x[i]
            numeric_x.append(date.year*12+date.month-s_date)
            
            #Correct negative values (change it to avegare of nearest values)
            if not i == len(x)-1 and y_data[i] <= 0 and y_data[i+1] <= 0:
                y_data[i] = (y_data[i-1]+y_data[i+2])/2
                y_data[i+1] = (y_data[i-1]+y_data[i+2])/2
            elif y_data[i] <= 0 and not i == 0 and not i == len(x)-1:
                y_data[i] = (y_data[i-1]+y_data[i+1])/2
            elif y_data[i] <= 0 and i == 0:
                y_data[i] = y_data[i+1]
            elif y_data[i] <= 0 and i == len(x)-1:
                y_data[i] = y_data[i-1]
        
        # Points on X-axis
        x_data = np.array(numeric_x) 

        # Trendline
        ylog_data = np.log(y_data)
        curve_fit = np.polyfit(x_data, ylog_data, 1)
        res_y = np.exp(curve_fit[0]*x_data) * np.exp(curve_fit[1])

        # AxisX numeric to DateTime
        numeric_x = []
        for arg in x_data:
            if arg%12 == 0:
                numeric_x.append(datetime.datetime(int(arg/12)+2015-1, 12, 1))
            else:
                numeric_x.append(datetime.datetime(int(arg/12)+2015, arg%12, 1))


        # Plots
        plt.plot(numeric_x, y_data, marker='o', markersize=2, label=name, color=colors[c])
        plt.plot(numeric_x,res_y, color=colors[c], linestyle='dashed', label="y = exp({:.5f}*x)*exp({:.2f})".format(curve_fit[0], curve_fit[1]))

        c += 1

    plt.legend(bbox_to_anchor=(-0.1, 1.1), loc='upper left', fontsize=8)

plt.show()
'''