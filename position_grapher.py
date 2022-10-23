import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.graph_objs as G
from plotly.subplots import make_subplots
import plotly
import common_utils.mongo_utils as mongo_utils
import boto3
import os
import io
import time
from common_utils.aws_tools import AWSHandler
from common_utils.constant import  MERGED_RAW_DATA_FILE_NAME, AWS_S3_BUCKET_NAME
from research_utils.dif_finder import delta_finder


def clean_file(file):
    file = file[file['rate'] != 'undefined']
    file = file.astype({'rate':'float'})

    return file

def create_flat_broker_dict(file,brokers):
    '''
    This function will take a file and return a dictionary that contains a flat df-
    for each broker, where a row contains its current bid, offer and price. 

    parameters:
    file: pandas dataframe
        the data frame of merged_raw_data
    brokers: array
        the list of relevant brokers
    '''
    
    df_dict = {}

    
    keep_columns = ["timestamp", "broker_name", "type", "rate", "size", "bbp_timestamp", "level"]
    file = file[keep_columns] 

    file.sort_values(by=["bbp_timestamp"], inplace=True)

    #create a datetime column
    '''file['datetime'] = pd.to_datetime(file.bbp_timestamp, unit='ms')'''

    # TODO: merge on BBP timestamp, add datetime later

    '''file = file.loc[(file["datetime"].dt.hour >= start_hour) & (file["datetime"].dt.hour < end_hour)]'''

    for broker in brokers:
        #create offers & bids df of level 0 of selected broker, leave bbp ts, merge on that
        offers = file.loc[(file['broker_name'] == broker) & (file['level'] == 0 ) & (file['type'] == 'offer')]
        bids = file.loc[(file['broker_name'] == broker) & (file['level'] == 0 ) & (file['type'] == 'bid')]

        offers = offers.drop(columns = [ 'type','level','timestamp','broker_name'])
        bids = bids.drop(columns = ['type','level','timestamp','broker_name'])
        
        offers = offers.rename(columns={'rate': f'{broker}_offer'})
        offers = offers.rename(columns={'size': f'{broker}_offer_size'})

        bids = bids.rename(columns={'rate': f'{broker}_bid'})
        bids = bids.rename(columns={'size': f'{broker}_bid_size'})

        merged_book = pd.merge(bids, offers,on=['bbp_timestamp'],suffixes=('_bid','_offer'))
        #merged_book.drop(columns=['broker_name_y'],inplace=True)
        #merged_book['broker_name'] = broker
        merged_book[f'{broker}_offer'].fillna(method='pad',inplace=True)
        merged_book[f'{broker}_bid'].fillna(method='pad',inplace=True)
        merged_book[f'{broker}_offer_size'].fillna(method='pad',inplace=True)
        merged_book[f'{broker}_bid_size'].fillna(method='pad',inplace=True)

        merged_book['datetime'] = pd.to_datetime(merged_book.bbp_timestamp, unit='ms')
        merged_book =merged_book.set_index('datetime')
        #merged_book['price'] = (merged_book['offer'] + merged_book['bid']) / 2
        df_dict[broker] = merged_book
    return df_dict

def merge_broker_dict(broker_dict):
    '''
    This function will take the broker dict from create_flat_broker_dict() and 
    make one unified flat df out of it, and add best offer and best bid columns

    ----------
    parameters:
    broker_dict: dict
        dict from create_flat_broker_dict(), containing a flat dataframe for each broker
    '''
    brokers = list(broker_dict.keys())
    cols = ['bbp_timestamp']
    bid_cols = []
    offer_cols = []
    for broker in brokers:
        print(f'{brokers.index(broker)} / {len(brokers)}')
        cols+=[f'{broker}_bid',f'{broker}_offer',f'{broker}_bid_size',f'{broker}_offer_size']
        bid_cols.append(f'{broker}_bid')
        offer_cols.append(f'{broker}_offer')
        if brokers.index(broker)==0:
            out_df = broker_dict[broker]
        else:
            out_df =  pd.merge(out_df, broker_dict[broker], how='outer',on='bbp_timestamp')
            #print(out_df.info(verbose=True))
    out_df =out_df[cols]
    '''for col in out_df.columns:
        #out_df[col].fillna(method='pad',inplace=True)
        out_df.pad(axis=0,inplace=True)'''

    # out_df['best_offer'] = out_df[offer_cols].min(axis=1)
    # out_df['best_offer_broker'] = out_df[offer_cols].idxmin(axis=1)

    # out_df['best_bid'] = out_df[bid_cols].max(axis=1)
    # out_df['best_bid_broker'] = out_df[bid_cols].idxmax(axis=1)

    out_df.sort_values(by=["bbp_timestamp"], inplace=True)
    for col in out_df.columns:
        out_df.pad(axis=0,inplace=True)
    return out_df

def delta_finder(delta_brokers, raw_df, delta_threshold):
    """
    Find delta opportunities in the raw dataframe based on the delta threshold and the delta brokers

    parameters:
    delta_threashold: float
        any delta between offer and bid lower than this value is considered dif
    raw_df: pandas dataframe
        the data frame of merged_raw_data
    delta_brokers: array
        the list of relevant brokers
    """

    print('starting to find difs')
    brokers_book = {}
    delta_list = []
    delta_id_counter = 0

    # initilize the book
    for broker_name in delta_brokers:
        brokers_book[broker_name] = {}
        brokers_book[broker_name]["bid"] = 0
        brokers_book[broker_name]["bid_size"] = 0

        brokers_book[broker_name]["offer"] = 0
        brokers_book[broker_name]["offer_size"] = 0

        brokers_book[broker_name]["bbp_timestamp"] = 0
        brokers_book[broker_name]["original_timestamp"] = 0

        brokers_book[broker_name]["offer_bbp_timestamp_last_change"] = 0
        brokers_book[broker_name]["bid_bbp_timestamp_last_change"] = 0

   # keep row where the broker_name is in delta_brokers and lewvel 0 only
    main_df = raw_df[(raw_df["level"] == 0) & (
        raw_df["broker_name"].isin(delta_brokers))]
    
    for bbp_timestamp, group in main_df.groupby("bbp_timestamp"):
       # loop over each row in the group
        for index, row in group.iterrows():
            # get the broker_name
            broker_name = row["broker_name"]
            # get the rate
            quote_rate = row["rate"]
            # get the type
            quote_type = row["type"]
            # get the original_timestamp
            quote_original_timestamp = row["original_timestamp"]
            # get the size
            quote_size = row["size"]
            # update brokers_book
            if quote_type == "bid":
                if brokers_book[broker_name]["bid"] == quote_rate and brokers_book[broker_name]["bid_size"] == quote_size:
                    pass
                else:
                    brokers_book[broker_name]["bid_bbp_timestamp_last_change"] = bbp_timestamp
                brokers_book[broker_name]["bid"] = quote_rate
                brokers_book[broker_name]["bbp_timestamp"] = bbp_timestamp
                brokers_book[broker_name]["original_timestamp"] = quote_original_timestamp
                brokers_book[broker_name]["bid_size"] = quote_size

            elif quote_type == "offer":
                if brokers_book[broker_name]["offer"] == quote_rate and brokers_book[broker_name]["offer_size"] == quote_size:
                    pass
                else:
                    brokers_book[broker_name]["offer_bbp_timestamp_last_change"] = bbp_timestamp
                brokers_book[broker_name]["offer"] = quote_rate
                brokers_book[broker_name]["bbp_timestamp"] = bbp_timestamp
                brokers_book[broker_name]["original_timestamp"] = quote_original_timestamp
                brokers_book[broker_name]["offer_size"] = quote_size
 # check if the new rates created an delta
        for index, row in group.iterrows():
            # get the broker_name
           broker_name = row["broker_name"]
           # get the type
           quote_type = row["type"]
           # get the bbp_timestamp
           quote_bbp_timestamp = row["bbp_timestamp"]
           for other_broker_name in delta_brokers:
                if other_broker_name != broker_name:
                    if quote_type == "bid" and brokers_book[other_broker_name]["offer"] != 0:
                        valid_rate = quote_bbp_timestamp - brokers_book[other_broker_name]["offer_bbp_timestamp_last_change"] < 40000 # should cahnge to 300 mls
                        if valid_rate:
                            try:
                                if brokers_book[other_broker_name]["offer"] - brokers_book[broker_name]["bid"] <= delta_threshold:
                                    # create an delta
                                    temp = {}
                                    temp["dif_name"] = f"{other_broker_name}-{broker_name}"
                                    temp["dif_value"] = np.round(brokers_book[other_broker_name]["offer"] - brokers_book[broker_name]["bid"],7)

                                    temp["offer_bbp_timestamp"] = brokers_book[other_broker_name]["bbp_timestamp"]
                                    temp["bid_bbp_timestamp"] = brokers_book[broker_name]["bbp_timestamp"]

                                    temp["offer_original_timestamp"] = brokers_book[other_broker_name]["original_timestamp"]
                                    temp["bid_original_timestamp"] = brokers_book[broker_name]["original_timestamp"]

                                    temp["offer_rate"] = brokers_book[other_broker_name]["offer"]
                                    temp["bid_rate"] = brokers_book[broker_name]["bid"]
                                    
                                    temp["dif_bbp_timestamp"] = quote_bbp_timestamp
                                    temp["id"] = f'{temp["dif_name"]}'

                                    if temp["offer_bbp_timestamp"] < temp["bid_bbp_timestamp"] and temp["offer_original_timestamp"] < temp["bid_original_timestamp"]:
                                        temp["direction_research"] = "buy"
                                        # temp["skew_broker"] =
                                    else:
                                        temp["direction_research"] = "none"
                                    delta_list.append(temp)
                                    delta_id_counter += 1
                            except Exception as e:
                                print('offer',brokers_book[other_broker_name]["offer"])
                                print('bid',brokers_book[broker_name]["bid"])
                                print(e)
                    elif quote_type == "offer" and brokers_book[other_broker_name]["bid"] != 0:
                        valid_rate = quote_bbp_timestamp - brokers_book[other_broker_name]["bid_bbp_timestamp_last_change"] < 40000
                        if valid_rate:
                            try:
                                if brokers_book[broker_name]["offer"] - brokers_book[other_broker_name]["bid"] <= delta_threshold:
                                    # create an delta
                                    temp = {}
                                    temp["dif_name"] = f"{broker_name}-{other_broker_name}"
                                    temp["dif_value"] = np.round(brokers_book[broker_name]["offer"] - brokers_book[other_broker_name]["bid"],7)

                                    temp["offer_bbp_timestamp"] = brokers_book[broker_name]["bbp_timestamp"]
                                    temp["bid_bbp_timestamp"] = brokers_book[other_broker_name]["bbp_timestamp"]

                                    temp["offer_original_timestamp"] = brokers_book[broker_name]["original_timestamp"]
                                    temp["bid_original_timestamp"] = brokers_book[other_broker_name]["original_timestamp"]

                                    temp["offer_rate"] = brokers_book[broker_name]["offer"]
                                    temp["bid_rate"] = brokers_book[other_broker_name]["bid"]

                                    temp["dif_bbp_timestamp"] = quote_bbp_timestamp
                                    temp["id"] = f'{temp["dif_name"]}'

                                    if temp["offer_bbp_timestamp"] > temp["bid_bbp_timestamp"] and temp["offer_original_timestamp"] > temp["bid_original_timestamp"]:
                                        temp["direction_research"] = "sell"
                                    else:
                                        temp["direction_research"] = "none"
                                    delta_list.append(temp)
                                    delta_id_counter += 1
                            except Exception as e:
                                print('offer',brokers_book[broker_name]["offer"])
                                print('bid',brokers_book[other_broker_name]["bid"])
                                print(e)
    return delta_list

def position_plots(difs, position, brokers,brokers_dict,merged,broker_groups, time_before_dif,time_after_dif,graph_other_brokers = True):
    '''
    This function takes an dif(dict) and a flat book with colunns of all brokers and index of ts and produces two graphs:
    - one of the bid and offer of each broker 3m before and after the dif
    - one with the size of bid and offer of each broker 3m before and after the dif

    -----------
    parameters:
    dif: dict
        an dif from dif_list, containing all info about one delta
    positions: array
        list of positions retreived from mongo
    brokers: array
        the list of relevant brokers
    brokers_dict:
        dict from create_flat_broker_dict(), containing a flat dataframe for each broker
    merged: df
        a flat df containing all prices and size for all brokers at a given time
    time_buffer: float
        how many mins before and after the dif should show on the graph
    graph_other_brokers: bool
        if to graph all other brokers that are not participating nor in delta
    
    '''
    def hex_to_rgba(h, alpha):
        '''
        converts color value in hex format to rgba format with alpha transparency
        '''
        return tuple([int(h.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)] + [alpha])
    dif_bbp_timestamp = position['dif_bbp_timestamp']
    
    lower_limit = dif_bbp_timestamp - 60000*time_before_dif
    upper_limit = dif_bbp_timestamp + 60000*time_after_dif


    graph_dif_name = ''
    for broker in position['dif_ids']:
        graph_dif_name+=broker
        if position['dif_ids'].index(broker) != len(position['dif_ids'])-1:
            graph_dif_name+='/'
    
    offer_columns = []
    bid_columns = []

    if not graph_other_brokers:
        offer_columns = []
        bid_columns = []
        for broker_name in brokers:
            if broker_name in [position['enter_broker'],position['exit_broker']]:
                bid_columns.append(broker_name+'_bid')
                offer_columns.append(broker_name+'_offer')
            elif broker_name in graph_dif_name:
                bid_columns.append(broker_name+'_bid')
                offer_columns.append(broker_name+'_offer')
    else:
        offer_columns = []
        bid_columns = []
        for col in merged.columns:
            if 'bid' in col and 'best' not in col and 'size' not in col:
                bid_columns.append(col)
            if 'offer' in col and 'best' not in col and 'size' not in col:
                offer_columns.append(col)


    merged_timeframe = merged.loc[(merged.bbp_timestamp >= lower_limit) & (merged.bbp_timestamp < upper_limit)]
    merged_timeframe = merged_timeframe.drop_duplicates()
    
    #print(len(merged_timeframe))
    max_offer = merged_timeframe[offer_columns].max().max()
    min_bid = merged_timeframe[bid_columns].min().min()


    # color list from which the colors of the lines will be generated
    colors_list = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", '#E3CF57','#8B2323','#7FFF00',"#1f77b4"]*2
    #enter_and_exit_colors = ['firebrick','royalblue']
    enter_and_exit_colors = ['orange','green','blue','red']


    #create graph and change settings
    price_plot = make_subplots(rows=1, cols=2, column_widths=[0.8, 0.2], specs=[[{"type": "xy"},{"type": "domain"}]])
    size_plot = make_subplots(rows=1, cols=2, column_widths=[0.8, 0.2], specs=[[{"type": "xy"},{"type": "domain"}]])

    price_plot.update_layout(title_text=f'price_plot {"".join(str(x) for x in position["dif_ids"])}')
    size_plot.update_layout(title_text=f'size_plot {"".join(str(x) for x in position["dif_ids"])}')

    price_plot.update_layout(legend=dict(groupclick="togglegroup"))
    size_plot.update_layout(legend=dict(groupclick="togglegroup"))

    price_plot.update_traces(hoverinfo='text+name', mode='lines')
    size_plot.update_traces(hoverinfo='text+name', mode='lines')

    color_index_to_use=0
    for broker_name in brokers:

        if broker_name in [position['enter_broker'],position['exit_broker']]:
            print('participant: ', broker_name)
            bid_col_name = broker_name + "_bid"
            offer_col_name = broker_name + "_offer"
            
            broker_df_timeframe = brokers_dict[broker_name].loc[(brokers_dict[broker_name].bbp_timestamp >= lower_limit) & (brokers_dict[broker_name].bbp_timestamp < upper_limit)]


            price_plot.add_trace(G.Scatter(legendgroup='participating broker',legendgrouptitle_text='participating broker',x=broker_df_timeframe.index, y=broker_df_timeframe[bid_col_name], name=bid_col_name,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
            price_plot.add_trace(G.Scatter(legendgroup='participating broker',x=broker_df_timeframe.index, y=broker_df_timeframe[offer_col_name], name=offer_col_name ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)

            size_plot.add_trace(G.Scatter(legendgroup='participating broker',legendgrouptitle_text='participating broker',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{bid_col_name}_size'], name=f'{bid_col_name}_size',line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
            size_plot.add_trace(G.Scatter(legendgroup='participating broker',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{offer_col_name}_size'], name=f'{offer_col_name}_size' ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)
            color_index_to_use+=1 #manual


        # brokers that are not in dif or trade
        elif broker_name not in graph_dif_name:
            if graph_difs_before_and_after:
                if broker_name in broker_groups['NY']:
                    # print(broker_name,'in NY')
                    bid_col_name = broker_name + "_bid"
                    offer_col_name = broker_name + "_offer"
                    
                    broker_df_timeframe = brokers_dict[broker_name].loc[(brokers_dict[broker_name].bbp_timestamp >= lower_limit) & (brokers_dict[broker_name].bbp_timestamp < upper_limit)]


                    price_plot.add_trace(G.Scatter(legendgroup='NY',legendgrouptitle_text='NY',x=broker_df_timeframe.index, y=broker_df_timeframe[bid_col_name], name=bid_col_name,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
                    price_plot.add_trace(G.Scatter(legendgroup='NY',x=broker_df_timeframe.index, y=broker_df_timeframe[offer_col_name], name=offer_col_name ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)

                    size_plot.add_trace(G.Scatter(legendgroup='NY',legendgrouptitle_text='NY',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{bid_col_name}_size'], name=f'{bid_col_name}_size',line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
                    size_plot.add_trace(G.Scatter(legendgroup='NY',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{offer_col_name}_size'], name=f'{offer_col_name}_size' ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)
                    color_index_to_use+=1 #manual
            if graph_other_brokers:
                # print('pusht',broker_name)
                bid_col_name = broker_name + "_bid"
                offer_col_name = broker_name + "_offer"
                
                broker_df_timeframe = brokers_dict[broker_name].loc[(brokers_dict[broker_name].bbp_timestamp >= lower_limit) & (brokers_dict[broker_name].bbp_timestamp < upper_limit)]


                price_plot.add_trace(G.Scatter(legendgroup='not in delta',legendgrouptitle_text='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[bid_col_name], name=bid_col_name,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
                price_plot.add_trace(G.Scatter(legendgroup='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[offer_col_name], name=offer_col_name ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)

                size_plot.add_trace(G.Scatter(legendgroup='not in delta',legendgrouptitle_text='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{bid_col_name}_size'], name=f'{bid_col_name}_size',line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
                size_plot.add_trace(G.Scatter(legendgroup='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{offer_col_name}_size'], name=f'{offer_col_name}_size' ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)
                color_index_to_use+=1 #manual
        else:
            # print('dif', broker_name)
            bid_col_name = broker_name + "_bid"
            offer_col_name = broker_name + "_offer"
            broker_df_timeframe = brokers_dict[broker_name].loc[(brokers_dict[broker_name].bbp_timestamp >= lower_limit) & (brokers_dict[broker_name].bbp_timestamp < upper_limit)]


            price_plot.add_trace(G.Scatter(legendgroup='in delta',legendgrouptitle_text='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[bid_col_name], name=bid_col_name,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
            price_plot.add_trace(G.Scatter(legendgroup='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[offer_col_name], name=offer_col_name ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)

            size_plot.add_trace(G.Scatter(legendgroup='in delta',legendgrouptitle_text='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{bid_col_name}_size'], name=f'{bid_col_name}_size',line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
            size_plot.add_trace(G.Scatter(legendgroup='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{offer_col_name}_size'], name=f'{offer_col_name}_size' ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)
            color_index_to_use+=1 #manual
    
    # adding the vertical line to mark the dif
    dif_datetime= pd.to_datetime(dif_bbp_timestamp,unit='ms')
    dif_nickname = 'delta'
    # size_plot.add_trace(G.Scatter(legendgroup='signal',legendgrouptitle_text='signal',x=[dif_datetime]*2, y=[min_bid,max_offer], name=f'{dif_nickname}' ,line_shape='hv', line=dict(width=2, color='black', dash="dash")), row=1, col=1)
    # price_plot.add_trace(G.Scatter(legendgroup='signal',legendgrouptitle_text='signal',x=[dif_datetime]*2, y=[min_bid,max_offer], name=f'{dif_nickname}' ,line_shape='hv', line=dict(width=2, color='black', dash="dash")), row=1, col=1)
    

    
    enter_request_text = f'enter request: {position["enter_broker"]} at {position["enter_order_requested_price"]}'
    enter_execute_text = f'enter execute: {position["enter_broker"]} at {position["enter_order_executed_price"]}'

    exit_request_text = f'exit request: {position["exit_broker"]} at {position["exit_order_requested_price"]}'
    exit_execute_text = f'exit execute: {position["exit_broker"]} at {position["exit_order_executed_price"]}'



    size_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['enter_order_request_timestamp']]*2, y=[min_bid,max_offer], name=enter_request_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[0], dash="dash")), row=1, col=1)
    size_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['enter_order_time']]*2, y=[min_bid,max_offer], name=enter_execute_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[1], dash="dash")), row=1, col=1)

    size_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['exit_order_request_timestamp']]*2, y=[min_bid,max_offer], name=exit_request_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[2], dash="dash")), row=1, col=1)
    size_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['exit_order_time']]*2, y=[min_bid,max_offer], name=exit_execute_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[3], dash="dash")), row=1, col=1)


    price_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['enter_order_request_timestamp']]*2, y=[min_bid,max_offer], name=enter_request_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[0], dash="dash")), row=1, col=1)
    price_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['enter_order_time']]*2, y=[min_bid,max_offer], name=enter_execute_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[1], dash="dash")), row=1, col=1)

    price_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['exit_order_request_timestamp']]*2, y=[min_bid,max_offer], name=exit_request_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[2], dash="dash")), row=1, col=1)
    price_plot.add_trace(G.Scatter(legendgroup='position',legendgrouptitle_text='position',x=[position['exit_order_time']]*2, y=[min_bid,max_offer], name=exit_execute_text ,line_shape='hv', line=dict(width=2, color=enter_and_exit_colors[3], dash="dash")), row=1, col=1)
    
    #Add points to enterance and exit

    size_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='position',x=[position['enter_order_request_timestamp']], y=[position['enter_order_requested_price']], name=f'{enter_request_text}' ,line_shape='hv', line=dict(width=3, color=enter_and_exit_colors[0], dash="dash")), row=1, col=1)
    size_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='position',x=[position['enter_order_time']], y=[position['enter_order_executed_price']], name=f'{enter_request_text}' ,line_shape='hv', line=dict(width=3, color=enter_and_exit_colors[1], dash="dash")), row=1, col=1)

    size_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='position',x=[position['exit_order_request_timestamp']], y=[position['exit_order_requested_price']], name=f'{exit_request_text}' ,line_shape='hv', line=dict(width=3, color=enter_and_exit_colors[0], dash="dash")), row=1, col=1)
    size_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='position',x=[position['exit_order_time']], y=[position['exit_order_executed_price']], name=f'{exit_execute_text}' ,line_shape='hv', line=dict(width=3, color=enter_and_exit_colors[1], dash="dash")), row=1, col=1)


    price_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='price_dots',x=[position['enter_order_request_timestamp']], y=[position['enter_order_requested_price']], name=f'{enter_request_text}' ,line_shape='hv', line=dict(width=3, color=enter_and_exit_colors[0], dash="dash")), row=1, col=1)
    price_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='price_dots',x=[position['enter_order_time']], y=[position['enter_order_executed_price']], name=f'{enter_execute_text}' ,line_shape='hv', line=dict(width=3, color=enter_and_exit_colors[1], dash="dash")), row=1, col=1)

    price_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='price_dots',x=[position['exit_order_request_timestamp']], y=[position['exit_order_requested_price']], name=f'{exit_request_text}', line=dict(width=3, color=enter_and_exit_colors[0], dash="dash")), row=1, col=1)
    price_plot.add_trace(G.Scatter(legendgroup='price_dots',legendgrouptitle_text='price_dots',x=[position['exit_order_time']], y=[position['exit_order_executed_price']], name=f'{exit_execute_text}' , line=dict(width=3, color=enter_and_exit_colors[1], dash="dash")), row=1, col=1)
    

    # From comment1 retreive info about the difs that caused the signal

    for signal_dif in position['signal_difs']:
        dif_nickname = f"{signal_dif['brokers'][0]}/{signal_dif['brokers'][1]}, {signal_dif['size']}, init: {signal_dif['initiating_broker']}"
        price_plot.add_trace(G.Scatter(legendgroup='signal',legendgrouptitle_text='signal',x=[pd.to_datetime(signal_dif['bbp_timestamp'],unit='ms')]*2, y=[min_bid,max_offer], name=f'{dif_nickname}' ,line_shape='hv', line=dict(width=2, color='black', dash="dash")), row=1, col=1)



    # adding NY difs that happened on the graph, before or after main dif

    before_after_color = '#bfbfbf'
    before_after_color = 'rgba'+ str(hex_to_rgba(before_after_color,0.75))
    if graph_difs_before_and_after:
        # print('graphing difs before and after')
        for before_dif in position['difs_before']:
            dif_text = f"{before_dif['dif_name']} | {before_dif['direction_research']} | {before_dif['dif_value']}"
            dif_datetime = pd.to_datetime(before_dif['dif_bbp_timestamp'],unit='ms')
            price_plot.add_trace(G.Scatter(legendgroup='NY Difs',visible = 'legendonly',legendgrouptitle_text='NY Difs',x=[dif_datetime]*2, y=[min_bid,max_offer], name=f'{dif_text}' ,line_shape='hv', line=dict(width=1, color=before_after_color, dash="dash")), row=1, col=1)
        for after_dif in position['difs_after']:
            dif_text = f"{after_dif['dif_name']} | {after_dif['direction_research']} | {after_dif['dif_value']}"
            dif_datetime = pd.to_datetime(after_dif['dif_bbp_timestamp'],unit='ms')
            price_plot.add_trace(G.Scatter(legendgroup='NY Difs',visible = 'legendonly',legendgrouptitle_text='NY Difs',x=[dif_datetime]*2, y=[min_bid,max_offer], name=f'{dif_text}' ,line_shape='hv', line=dict(width=1, color=before_after_color, dash="dash")), row=1, col=1)
        
    



    price_plot.update_traces(
    hovertemplate="<br>".join([
        "Time: %{x}",
        "Rate: %{y}",
    ]))
    delta_ids = ''
    for s in difs:
        delta_ids= delta_ids+f'<br> - {s}'
    loser=''
    if round(position["revenue_pips"],7) < 0:
        loser = '<br>LOSER!'
    price_plot.add_annotation(
        text=f'''direction: {position["direction"]}<br>
        Israel Time: {pd.to_datetime(dif_bbp_timestamp,unit="ms")+timedelta(hours=3)}<br>
        Internal Latency: {position["internal_latency"]}<br>
        Enter Drift: {'{:f}'.format(round(position["enter_order_executed_price"]-position["enter_order_requested_price"],7))} <br>
        Exit Drift: {'{:f}'.format(round(position["exit_order_executed_price"]-position["exit_order_requested_price"],7))} <br>
        Quantity: {position["enter_order_executed_size"]} <br>
        initiating broker: <br> - {position["initiating_broker"]}<br>
        Dif ids: {delta_ids}<br>revenue: {position["revenue"]}<br>
        revenue Pips: {round(position["revenue_pips"],7)}<br>
        Enter Trade id: {position["enter_trade_id"]}<br>
        Exit Trade id: {position["exit_trade_id"]}''', 
                    align='left',
                    showarrow=False,
                    xref='paper',
                    yref='paper',
                    x=1.0,
                    y=1.0,
                    bordercolor='black',
                    borderwidth=1)
    # print(type(price_plot))
    return price_plot,size_plot

def delta_plots(dif, brokers,brokers_dict,merged, time_buffer=0.5,graph_other_brokers = True):
    '''

    -----------
    parameters:
    dif: dict
        an dif from dif_list, containing all info about one delta
    brokers: array
        the list of relevant brokers
    brokers_dict:
        dict from create_flat_broker_dict(), containing a flat dataframe for each broker
    merged: df
        a flat df containing all prices and size for all brokers at a given time
    time_buffer: float
        how many mins before and after the dif should show on the graph
    graph_other_brokers: bool
        if to graph all other brokers that are not participating nor in delta
    
    '''

    dif_bbp_timestamp = dif['dif_bbp_timestamp']
    
    lower_limit = dif_bbp_timestamp - 60000*time_buffer
    upper_limit = dif_bbp_timestamp + 60000*time_buffer

    bbp_datetime = pd.to_datetime(dif_bbp_timestamp)

    offer_columns = []
    bid_columns = []
    for col in merged.columns:
        if 'bid' in col and 'best' not in col and 'size' not in col:
            bid_columns.append(col)
        if 'offer' in col and 'best' not in col and 'size' not in col:
            offer_columns.append(col)


    merged_timeframe = merged.loc[(merged.bbp_timestamp >= lower_limit) & (merged.bbp_timestamp < upper_limit)]
    merged_timeframe = merged_timeframe.drop_duplicates()
    
    max_offer = merged_timeframe[offer_columns].max().max()
    min_bid = merged_timeframe[bid_columns].min().min()


    # color list from which the colors of the lines will be generated
    colors_list = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", '#E3CF57','#8B2323','#7FFF00',"#1f77b4"]*2
    #enter_and_exit_colors = ['firebrick','royalblue']
    


    #create graph and change settings
    price_plot = make_subplots(rows=1, cols=2, column_widths=[0.8, 0.2], specs=[[{"type": "xy"},{"type": "domain"}]])
    size_plot = make_subplots(rows=1, cols=2, column_widths=[0.8, 0.2], specs=[[{"type": "xy"},{"type": "domain"}]])


    price_plot.update_layout(title_text=f'price_plot {dif["dif_name"]}')
    size_plot.update_layout(title_text=f'size_plot {dif["dif_name"]}')

    price_plot.update_layout(legend=dict(groupclick="togglegroup"),hoverdistance=1000000000)
    size_plot.update_layout(legend=dict(groupclick="togglegroup"),hoverdistance=1000000000)

    price_plot.update_traces(hoverinfo='text+name', mode='lines')
    size_plot.update_traces(hoverinfo='text+name', mode='lines')

    price_plot.update_layout(
    font=dict(
        size=11  
    ))
    color_index_to_use=0
    for broker_name in brokers:
        #print(broker_name)
        if broker_name not in dif['dif_name']:
            if graph_other_brokers:
                #print('pusht',broker_name)
                bid_col_name = broker_name + "_bid"
                offer_col_name = broker_name + "_offer"
                
                broker_df_timeframe = brokers_dict[broker_name].loc[(brokers_dict[broker_name].bbp_timestamp >= lower_limit) & (brokers_dict[broker_name].bbp_timestamp < upper_limit)]


                price_plot.add_trace(G.Scatter(legendgroup='not in delta',legendgrouptitle_text='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[bid_col_name], name=bid_col_name,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
                price_plot.add_trace(G.Scatter(legendgroup='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[offer_col_name], name=offer_col_name ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)

                size_plot.add_trace(G.Scatter(legendgroup='not in delta',legendgrouptitle_text='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{bid_col_name}_size'], name=f'{bid_col_name}_size',line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
                size_plot.add_trace(G.Scatter(legendgroup='not in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{offer_col_name}_size'], name=f'{offer_col_name}_size' ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)
                color_index_to_use+=1 #manual
        else:
            #print('dif', broker_name)
            bid_col_name = broker_name + "_bid"
            offer_col_name = broker_name + "_offer"
            broker_df_timeframe = brokers_dict[broker_name].loc[(brokers_dict[broker_name].bbp_timestamp >= lower_limit) & (brokers_dict[broker_name].bbp_timestamp < upper_limit)]


            price_plot.add_trace(G.Scatter(legendgroup='in delta',legendgrouptitle_text='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[bid_col_name], name=bid_col_name,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
            price_plot.add_trace(G.Scatter(legendgroup='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[offer_col_name], name=offer_col_name ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)

            size_plot.add_trace(G.Scatter(legendgroup='in delta',legendgrouptitle_text='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{bid_col_name}_size'], name=f'{bid_col_name}_size',line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use], dash="dash")), row=1, col=1)
            size_plot.add_trace(G.Scatter(legendgroup='in delta',x=broker_df_timeframe.index, y=broker_df_timeframe[f'{offer_col_name}_size'], name=f'{offer_col_name}_size' ,line_shape='hv', line=dict(width=1, color=colors_list[color_index_to_use])), row=1, col=1)
            color_index_to_use+=1 #manual
    
    # adding the vertical line to mark the dif
    dif_datetime= pd.to_datetime(dif_bbp_timestamp,unit='ms')
    dif_nickname = dif['dif_name']
    size_plot.add_trace(G.Scatter(legendgroup='delta',legendgrouptitle_text='delta',x=[dif_datetime]*2, y=[min_bid,max_offer], name=f'{dif_nickname}' ,line_shape='hv', line=dict(width=2, color='black', dash="dash")), row=1, col=1)
    price_plot.add_trace(G.Scatter(legendgroup='delta',legendgrouptitle_text='delta',x=[dif_datetime]*2, y=[min_bid,max_offer], name=f'{dif_nickname}' ,line_shape='hv', line=dict(width=2, color='black', dash="dash")), row=1, col=1)
    

    
    price_plot.update_traces(
    hovertemplate="<br>".join([
        "Time: %{x}",
        "Rate: %{y}",
    ]))
    return price_plot,size_plot

def sync_positions_and_difs(position_list, dif_list,time_before_dif,time_after_dif,broker_groups):
    '''
    This cube makes sure every position has a found dif, and if the dif matches by time and by 
    broker to the positions, the position's _id is assigned to the dif

    IF DATA FORMATING CHANGES, THIS IS VOLUNERABLE

    -----------
    parameters:
    dif_list: dict
        dictionary containing all of the day's deltas
    position_list: array
        list of positions retreived from mongo
    '''
    print('syncing positions')
    mismatches = []
    for pos in position_list:
        match_direction = False
        ts = pos['dif_bbp_timestamp']
        dif_found = False
        dif_found_ts_based = False
        dif_found_ts_broker_based = False

        #lists for difs before and after the signals. 
        difs_before = []
        difs_after = []
        #set upper and lower limit
        lower_limit = ts - 60000*time_before_dif
        upper_limit = ts + 60000*time_after_dif

        for dif in dif_list:
            if int(dif['dif_bbp_timestamp']) == int(ts):
                dif_found_ts_based = True
                dif_brokers = dif['dif_name'].split('-')
                dif_brokers_swapped = [dif_brokers[1],dif_brokers[0]]
                if dif_brokers in pos['broker_pairs'] or dif_brokers_swapped in pos['broker_pairs']:
                    dif_found_ts_broker_based = True
                    if dif['direction_research'] == pos['direction']:
                        dif_found = True
                        #dif['position_id'] = str(pos['_id'])
                        dif.update({'position_id':str(pos['_id'])})
                        pos['dif_ids'] = pos['dif_ids'] + [dif['id']]
                        print(f"Dif and position match found: {pos['_id']}")
                    else:
                        print('direction mistmatch')
                        print(f'reaserch {dif["direction_research"]} vs {pos["direction"]}')
                        try:
                            x = dif['position_id']
                        except:
                            dif['position_id'] = str(-1)
                else:
                    print('broker mistmatch')
                    print(f'{dif_brokers_swapped} not found in {pos["broker_pairs"]}')
                    try:
                        x = dif['position_id']
                    except:
                        dif['position_id'] = str(-1)
            else:
                try:
                    x = dif['position_id']
                except:
                    dif['position_id'] = str(-1)


            
            

            # find any difs between lower and upper limit and add the to before and after lists
            if dif['dif_bbp_timestamp'] > lower_limit and dif['dif_bbp_timestamp'] < ts and dif['dif_name'].split('-')[0] in broker_groups['NY']:
            # if dif['dif_bbp_timestamp'] >= lower_limit and dif['dif_bbp_timestamp'] < ts:
                # print(f'found before: {dif["dif_name"].split("-")[0]}')
                difs_before.append(dif)

            if dif['dif_bbp_timestamp'] < upper_limit and dif['dif_bbp_timestamp'] > ts and dif['dif_name'].split('-')[0] in broker_groups['NY']:
            # if dif['dif_bbp_timestamp'] <= upper_limit and dif['dif_bbp_timestamp'] > ts:
                # print(f"found after: {dif['dif_name'].split('-')[0]}")
                difs_after.append(dif)

        # adding these difs to the position
        pos['difs_before'] = difs_before
        pos['difs_after'] = difs_after

        # print(f"{pos['_id']} | before: {len(difs_before)} | after: {len(difs_after)}")
        if dif_found:
            x=0
        elif not dif_found and dif_found_ts_broker_based:
            print(f"Dif and Position direction mismatch. id is: {pos['_id']}. time is: {pos['enter_order_request_timestamp']}")
            mismatches.append(f"Dif and Position direction mismatch. id is: {pos['_id']}. time is: {pos['enter_order_request_timestamp']}")

        elif not dif_found and dif_found_ts_based:
            print(f"Dif and Position broker mismatch. id is: {pos['_id']}. time is: {pos['enter_order_request_timestamp']}")
            mismatches.append(f"Dif and Position broker mismatch. id is: {pos['_id']}. time is: {pos['enter_order_request_timestamp']}")

        elif not dif_found and not dif_found_ts_based:
            print(f"Dif and Position timestamp mismatch. id is: {pos['_id']}. ts is: {pos['dif_bbp_timestamp']}. {pos['broker_pairs']}")
            for dif in dif_list:
                if dif['dif_bbp_timestamp'] == pos['dif_bbp_timestamp']:
                    print('found, this is bad!1')
            mismatches.append(pos['dif_bbp_timestamp'])
        elif not dif_found_ts_based:
            print('this is lowk bad')

    print('----- MISMATCHES: ------')
    for m in mismatches:
        print(m)
        for dif in dif_list:
            if dif['dif_bbp_timestamp'] == m:
                print('found, this is bad!')

    print('------ analysis: -------')
    counter = 0
    for pos in position_list:
        if len(pos['dif_ids']) > 0:
            counter+=1
    print(f"{counter} / {len(position_list)} have dif ids")
    counter = 0
    for pos in position_list:
        for dif in dif_list:
            if int(dif['dif_bbp_timestamp']) == int(pos['dif_bbp_timestamp']):
                counter+=1
    print(f'counter {counter}')
    print('done syncing positions')
    
def sync_signal_no_position_and_dif_list(signal_no_position,dif_list,broker_groups):
    '''
    This cube makes sure every failed position has a found dif, and if the dif matches by time and by 
    broker to the failed positions, the failed_position's _id is assigned to the dif

    IF DATA FORMATING CHANGES, THIS IS VOLUNERABLE

    -----------
    parameters:
    dif_list: dict
        dictionary containing all of the day's deltas
    signal_no_position: array
        list of failed positions retreived from mongo
    '''
    print('syncing failed positions')
    for failed_position in signal_no_position:
        ts = failed_position['dif_bbp_timestamp']
        dif_found = False
        for dif in dif_list:
            if dif['dif_bbp_timestamp'] == ts:
                dif_brokers = dif['dif_name'].split('-')
                for ind in range(len(dif_brokers)):
                    if 'LONDON' in dif_brokers[ind]:
                        dif_brokers[ind] =  dif_brokers[ind].split('_')[1]
                    if 'NY' in dif_brokers[ind]:
                        dif_brokers[ind] =dif_brokers[ind].replace('BROKER_','')
                
                dif_brokers_swapped = [dif_brokers[1],dif_brokers[0]]

                if dif_brokers in failed_position['broker_pairs'] or dif_brokers_swapped in failed_position['broker_pairs']:

                    dif_found = True

                    print(f"Dif and failed position match found: {dif['id']}, {failed_position['broker_pairs']}, {dif['dif_name']}")
                    # print(f"timestamp match: {dif['dif_bbp_timestamp']}, {failed_position['dif_bbp_timestamp']}")

                    #dif['position_id'] = str(pos['_id'])
                    dif.update({'failed_position_id':str(failed_position['_id'])})
                    dif['failed_position_id'] = str(failed_position['_id'])
                    failed_position['dif_ids'] = failed_position['dif_ids'] + [dif['id']]
                    #log(f"{pos['_id']}, vs, {dif['position_id']}")
                else:
                    try:
                        x = dif['failed_position_id']
                    except:
                        dif['failed_position_id'] =str(-1)
                        dif.update({'failed_position_id':str(-1)})
                        #print('-')
            else:
                try:
                    x = dif['failed_position_id']
                except:
                    dif['failed_position_id'] =str(-1)
                    dif.update({'failed_position_id':str(-1)})
                    #print('-')
        if not dif_found:
            x=0
            #print(f"Dif and failed position match not found. time is: {failed_position['datetime']}")
    print('done syncing failed positions')

def sync_interesting_deltas(dif_list,broker_groups,position_list,merged, failed_position_list,brokers,brokers_dict,working_date,aws_handler,raw_positions,raw_signal_no_position,time_before_dif,time_after_dif,graph_other_brokers):
    '''
    This cube makes sure every position has a found dif, and if the dif matches by time and by 
    broker to the positions, the position's _id is assigned to the dif

    IF DATA FORMATING CHANGES, THIS IS VOLUNERABLE

    -----------
    parameters:
    dif_list: dict
        dictionary containing all of the day's deltas
    position_list: array
        list of positions retreived from mongo
    failed_position_list: array
        list of failed positions retreived from mongo
    brokers: array
        the list of relevant brokers
    brokers_dict:
        dict from create_flat_broker_dict(), containing a flat dataframe for each broker
    working_date: sting
        stiring of the date {DD-MM-YYYY}
    merged: df
        a flat df containing all prices and size for all brokers at a given time
    aws_handler: AWSHandler object
        object containing tools to retreive and upload files from/to S3
    '''
    print('started syncing interesting difs')
    for pos in position_list:
        if len(pos['dif_ids']) == 0:
            x=0
            # try:
            #     print('position does not have difs',raw_positions[failed_position_list.index(pos)]['comment1'])
            # except:
            #     print('failed position does not have difs',pos['_id'])
        else:
            price_plot,size_plot = position_plots(difs=pos['dif_ids'],broker_groups=broker_groups,merged=merged,position = pos,brokers=brokers,time_after_dif=time_after_dif,time_before_dif=time_before_dif,graph_other_brokers=graph_other_brokers,brokers_dict=brokers_dict)
            graph_name = f'{pos["_id"]}'
            html_string = plotly.io.to_html(price_plot)

            aws_handler.save_html_file_in_bucket(working_date=f'{working_date}', html_string=html_string,file_name_to_save=f'positions/{graph_name}.html',bucket_name = f"delta-info-graphs")
        
def get_mongo_positions_delta_lists(date):
    position_list,raw_positions = mongo_utils.retreive_position_dicts(date)
    signal_no_position,raw_signal_no_position = mongo_utils.retreive_delta_dict_mongo(date)
    return position_list,signal_no_position,raw_positions,raw_signal_no_position

def text_to_datetime(txt):
    '''
    takes text {DD-MM-YYYY} and converts to datetime

    ----------
    parameters:
    txt: txt
        {DD-MM-YYYY} 
    '''
    split = txt.split('-')
    year = int(split[2])
    month = int(split[1])
    day = int(split[0])
    return datetime(year, month, day)

def create_delta_csv(dif_list,working_date,aws_handler,broker_groups,ran_dif_ceiling):
    '''
    Creates a csv made of dif_list, containig all the info about all of the day's deltas

    -----------
    parameters:
    dif_list: dict
        {dictionary containing all of the day's deltas}
    working_date: sting
        {stiring of the date: DD-MM-YYYY}
    aws_handler: AWSHandler object
        object containing tools to retreive and upload files from/to S3
    '''
    lst = []
    for dif in dif_list:
        if dif['dif_value'] < ran_dif_ceiling:
            try:
                if dif['failed_position_id'] == -1:
                    dif['failed_position_id'] = 0
                if dif['position_id'] == -1:
                    dif['position_id'] = 0
            except Exception as e:
                print('create delta csv ERROR:', e)
                dif['failed_position_id'] = -2
                dif['position_id'] = -2
        for key in broker_groups.keys():
            if dif['dif_name'].split('-')[0] in broker_groups[key]:
                dif['group'] = key
        lst.append(dif)
    df = pd.DataFrame(lst)
    df.sort_values(by='dif_bbp_timestamp', inplace=True)
    aws_handler.save_file_in_bucket(working_date=f'{working_date}', df_to_save=df,file_name_to_save='delta_summary.csv',bucket_name = f"delta-info-graphs")

def from_datetime(date):
    '''
    takes text datetime and converts to {DD-MM-YYYY}

    ----------
    parameters:
    broker_dict: datetime
        date
    '''
    month = str(date.month)
    day = str(date.day)
    if len(month) == 1:
        month = '0'+month
    if len(day) == 1:
        day = '0'+day
    s = f'{day}-{month}-{date.year}'
    return s

def read_df_by_full_file_path(bucket_name, full_file_path):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=full_file_path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    df_data = pd.read_csv(response.get("Body"))
    return df_data, status

def retreive(date):
    '''
    retrieves file from s3

    ----------
    parameters:
    date: txt
        {DD-MM-YYYY} 
    '''

    #DD-MM-YYYY
    raw_data_file_path = os.path.join(date,MERGED_RAW_DATA_FILE_NAME)
    print(raw_data_file_path)
    try:
        raw_data, upload_status = read_df_by_full_file_path(AWS_S3_BUCKET_NAME, raw_data_file_path)
        print('retreived file')
        return raw_data
    except Exception as e:
        print(e)
        print('failed to retreive file')

def create_delta_graphs_and_csv(today,start_hour, end_hour,broker_groups,dif_threashold,ran_dif_ceiling,dif_file_exists,dif_file,graph_other_brokers,time_before_dif,time_after_dif):
    '''
    incorporates all the functions above to create graphs for each dif matching with a position/failed positions and save on S3
    creates csv containing all the info about the day's deltas and save on S3
    ----------
    parameters:
    today: datetime
        date
    start_hour: int
        start of timeframe
    end_hour:
        end of timeframe
    '''
    position_list,signal_no_position,raw_positions,raw_signal_no_position = get_mongo_positions_delta_lists(today)

    print('IMPORTED MONGO')
    working_date = from_datetime(today)

    # pull the merged raw data file
    
    file = retreive(working_date)
    
    # print(f'finished reading file ')

    
    file = clean_file(file)
    print('finished cleaining file')
    #for now there is a timeframe!
    file = file.loc[(pd.to_datetime(file["bbp_timestamp"],unit='ms').dt.hour >= start_hour) & (pd.to_datetime(file["bbp_timestamp"],unit='ms').dt.hour < end_hour)]


    #generate brokers list
    if not graph_other_brokers:
        brokers = []
        for key in broker_groups.keys():
            brokers += broker_groups[key]
    else:
        brokers = file.broker_name.unique()

    brokers = file.broker_name.unique()
    brokers_dict = create_flat_broker_dict(file=file,brokers=brokers)
    print(f'finished broker_dict ')
    
    merged = merge_broker_dict(brokers_dict)
    print(f'finished merging dicts ')

    
    
    dif_list=[]

    tic = time.perf_counter()

    if not dif_file_exists:
        
        for key in broker_groups.keys():
            filter = file['broker_name'].isin(broker_groups[key])
            broker_filtered_df = file[filter]
            dif_list = dif_list + delta_finder(broker_groups[key], broker_filtered_df, dif_threashold)
        toc = time.perf_counter()
        print(f'finished finding deltas in {toc - tic:0.4f}')
    else:
        dif_list = dif_file.to_dict('records')
        print('imported difs')



    # sync between failed positions and positions and difs. This will add position_id/failed_position_id field 
    # to difs that match with the positions in mongo
    
    sync_signal_no_position_and_dif_list(signal_no_position,dif_list,broker_groups=broker_groups)
    sync_positions_and_difs(position_list,dif_list,time_before_dif,time_after_dif,broker_groups=broker_groups)

    aws_handler = AWSHandler(f"create_daily_delta_info_graphs")
    #sync and add graphs to interesting deltas
    sync_interesting_deltas(
        dif_list,
        brokers_dict=brokers_dict,
        merged=merged,
        failed_position_list=signal_no_position,
        position_list=position_list,
        brokers= brokers,
        working_date=working_date,
        aws_handler=aws_handler,
        raw_positions= raw_positions,
        raw_signal_no_position=raw_signal_no_position,
        graph_other_brokers=graph_other_brokers,
        time_before_dif=time_before_dif,
        time_after_dif=time_after_dif,
        broker_groups=broker_groups)
    
    print(f'synced difs ')
    
    create_delta_csv(dif_list=dif_list,working_date=working_date,aws_handler=aws_handler,broker_groups=broker_groups,ran_dif_ceiling=ran_dif_ceiling)

graph_difs_before_and_after = True

#-----------VARIABLES-----------
def delta_info_graphs_main(dates):
    print('Starting delta_info_graphs')
    #todays date 

    

    #threashold for detecting deltas
    dif_threashold = -0.00001

    # graph all other brokers that are not participating nor in delta
    graph_other_brokers = False
    #info about where the merged raw data is saved


    #timeframe, this is UTC time, take 2hrs back from IL time
    start_hour=7
    end_hour=18

    ran_dif_ceiling=-0.0001

    broker_groups={
        'NY':['BROKER_NY_A', 'BROKER_NY_B', 'BROKER_NY_C'],
        'LONDON':[f'BROKER_LONDON{i+1}' for i in range(21) if i != 13]
        ,'OTHER':['PARIS', 'BERLIN', 'BARCELONA']
    }

    #time buffer for graphs, how many mins before and after the dif should show on the graph
    time_before_dif = 0.5
    time_after_dif = 2

    dif_file_exists = False
    dif_file,status = read_df_by_full_file_path(f"delta-info-graphs", f'12-09-2022/delta_summary.csv')

    graph_difs_before_and_after = True
    #-------------------------------

    for date in dates:
        today = text_to_datetime(date)
        create_delta_graphs_and_csv(
            today,
            start_hour=start_hour,
            end_hour=end_hour,
            broker_groups=broker_groups,
            dif_threashold=dif_threashold,
            ran_dif_ceiling=ran_dif_ceiling,
            time_before_dif=time_before_dif,
            time_after_dif=time_after_dif,
            dif_file_exists=dif_file_exists,
            dif_file=dif_file,
            graph_other_brokers=graph_other_brokers)
        print('FINISHED:', date)

