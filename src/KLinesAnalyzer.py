import numpy as np
from binance.client import Client
from src.KLines import KLines
import os
import json


class KLinesAnalyzer:
    def __init__(self, coin_name, currency, config, from_time, to_time, json_save_path=None, interval_idx=11):
        self.coin_market = coin_name + currency
        self.from_time = from_time
        self.to_time = to_time
        self.interval_idx = interval_idx
        self.client = Client(config.APIKEY, config.APISECRET)
        if json_save_path is None:
            self.json_save_path = os.getcwd()
        else:
            self.json_save_path = json_save_path

        # The UTC + 0 timestamp
        self.klines_array = KLines(self.get_klines())
        self.price_array = self.klines_array.get_price_array()

        # Different matrix with column 0 - close, 1 - high, 2 - low
        self.diff_matrix = self.__get_price_diff_percent()

    @staticmethod
    def get_diff_percent_static(*args, base_array=None):
        if base_array is None:
            base_array = args[0]
            compensate_idx = 1
        else:
            compensate_idx = 0
        output_arr = np.array([])
        for i in range(compensate_idx, len(args)):
            if isinstance(args[i], np.ndarray):
                temp_percent = np.divide(np.subtract(args[i], base_array), base_array)
                output_arr = np.append(output_arr, temp_percent)
            else:
                raise ValueError("The type of the input at the {}-th index is not np.ndarray".format(i))

        return output_arr.reshape(len(args) - compensate_idx, -1).T

    @staticmethod
    def mkdir(path, dir_name):
        path_dir_name = os.path.join(path, dir_name)
        try:
            os.mkdir(path_dir_name)
        except OSError:
            print("Creation of the temperature with time directory %s failed" % path_dir_name)
        else:
            print("Successfully created the  temperature with time directory %s" % path_dir_name)

    @staticmethod
    def get_top_min(array, top_length=10):
        return array[array.argsort()[:top_length]]

    @staticmethod
    def get_top_max(array, top_length=10):
        return array[(-array).argsort()[:top_length]]

    @staticmethod
    def get_change_percentage(base, compare):
        return abs(compare - base) / base * 100

    def __get_price_diff_percent(self):
        output_arr = self.get_diff_percent_static(self.klines_array.get_close_price(),
                                                  self.klines_array.get_high_price(),
                                                  self.klines_array.get_low_price(),
                                                  base_array=self.klines_array.get_open_price())
        return output_arr

    def filter_period_by_price(self, period_list, filter_percentage=2):
        """
        Get all periods which have the price change in percentage >= filter_percentage
        :param period_list: List of period
        :param filter_percentage: The low threshold to filter period
        :return:
        """
        filtered_period_list = list()
        for one_period in period_list:
            if self.get_change_percentage(self.klines_array.get_open_price(one_period[0]),
                                          self.klines_array.get_open_price(one_period[1])) >= filter_percentage:
                filtered_period_list.append(one_period)
            else:
                pass
        return filtered_period_list

    def __sort_period_by_price(self, period):
        period.sort(reverse=True, key=lambda x: self.get_change_percentage(self.klines_array.get_open_price(x[0]),
                                                                           self.klines_array.get_open_price(x[1])))

    def __create_period_json_data(self, period):
        """
        Increase period data structure
        {
            "start_timestamp": "2022-01-31 01:00:00",
            "end_timestamp": "2022-01-31 02:15:00",
            "start_price": 2.3732,
            "end_price": 2.6104,
            "increase_percent": 9.99,
            "volume": 9455258.0,
            "buy_volume": 4824587.0,
            "buy_percent": 51.03
        }
        :param period: period start and end index
        :return:
        """
        output_dict = dict()
        output_dict["start_timestamp"] = str(self.klines_array.get_open_time_readable(period[0]))
        output_dict["end_timestamp"] = str(self.klines_array.get_open_time_readable(period[1]))
        output_dict["start_price"] = self.klines_array.get_open_price(period[0])
        output_dict["end_price"] = self.klines_array.get_open_price(period[1])
        output_dict["increase_percent"] = round(self.get_change_percentage(output_dict["start_price"],
                                                                           output_dict["end_price"]), 2)
        output_dict["volume"] = self.klines_array.get_volume()[period[0]:period[1]].sum()
        output_dict["buy_volume"] = self.klines_array.get_buy_base_asset()[period[0]:period[1]].sum()
        output_dict["buy_percent"] = round(output_dict["buy_volume"] / output_dict["volume"] * 100, 2)
        return output_dict

    def dump_json_file(self, period, dir_name):
        # Make save dir
        self.mkdir(self.json_save_path, dir_name)
        output_all = list()

        # Create save object for all period
        for i in range(len(period)):
            temp_dict = self.__create_period_json_data(period[i])
            output_all.append(temp_dict)

        json_all_name = "{}_{}_{}_all.json".format(self.from_time.replace(" ", "_"),
                                                   self.to_time.replace(" ", "_"),
                                                   self.coin_market)
        json_all_path = os.path.join(self.json_save_path, dir_name, json_all_name)

        # Create save object for filtered periods
        filter_period = self.filter_period_by_price(period)
        output_filter = list()
        for i in range(len(filter_period)):
            temp_dict = self.__create_period_json_data(filter_period[i])
            output_filter.append(temp_dict)

        json_filter_name = "{}_{}_{}_filter.json".format(self.from_time.replace(" ", "_"),
                                                         self.to_time.replace(" ", "_"),
                                                         self.coin_market)
        json_filter_path = os.path.join(self.json_save_path, dir_name, json_filter_name)

        # Create save object for filtered periods and sorted by the percentage of price changes
        self.__sort_period_by_price(filter_period)
        output_sort = list()
        for i in range(len(filter_period)):
            temp_dict = self.__create_period_json_data(filter_period[i])
            output_sort.append(temp_dict)

        json_sort_name = "{}_{}_{}_filter_sort.json".format(self.from_time.replace(" ", "_"),
                                                            self.to_time.replace(" ", "_"),
                                                            self.coin_market)
        json_sort_path = os.path.join(self.json_save_path, dir_name, json_sort_name)

        # Save all above object
        with open(json_all_path, "w") as file:
            json.dump(output_all, file, indent=4)

        with open(json_filter_path, "w") as file:
            json.dump(output_filter, file, indent=4)

        with open(json_sort_path, "w") as file:
            json.dump(output_sort, file, indent=4)

    def save_notable_period(self, option=3):
        """
        :param option: 1 - Increase, 2 - Decrease, 3 - Both
        The method dumps a list of period data into Json file
        """
        # Generate json file for all period
        if option == 1 or option == 3:
            self.dump_json_file(self.get_increase_period(0.85), "json_data_raise")
        elif option == 2 or option == 3:
            self.dump_json_file(self.get_decrease_period(0.15), "json_data_plummet")
        else:
            raise ValueError("The current value of option param is not correct. Its value must be between 0 and 3")

    def get_increase_period(self, tolerance_percent):
        """
        :param tolerance_percent: The limit of decrease to stop the raise sequence (for example: if current price is
        lower than (tolerance_percent * highest_price) in the current considering period it will end the raise period)
        :return: List of pair start period and end period index (for example [[0, 1], [11, 23], [33, 38]])
        """
        close = self.diff_matrix[:, 0]
        length = close.shape[0]
        output_list = []
        cur_accum_change = 0.0
        highest_accum_change = 0.0
        for i in range(length):
            cur_accum_change += close[i]
            if cur_accum_change < 0:
                if len(output_list) != 0 and output_list[-1][1] == -1:
                    output_list[-1][1] = i
                cur_accum_change = 0.0
                highest_accum_change = 0.0
                continue
            elif output_list.__len__() == 0 or output_list[-1][1] != -1:
                output_list.append([i, -1])
            else:
                pass

            if cur_accum_change < highest_accum_change:
                if cur_accum_change < (tolerance_percent * highest_accum_change) or i == length-1:
                    output_list[-1][1] = i
                    cur_accum_change = 0.0
                    highest_accum_change = 0.0
            elif i == length - 1:
                if output_list[-1][0] == i:
                    output_list.pop()
                else:
                    output_list[-1][1] = i
            else:
                highest_accum_change = cur_accum_change
        return output_list

    def get_decrease_period(self, tolerance_percent):
        """
        :param tolerance_percent: The limit of decrease to stop the plummeted sequences (for example: if the current
        price higher than (tolerance_percent * lowest_price) in the current considering period ir will end the
        plummeted sequence.
        :return: List of pair of start and end period indexes (for example [[0, 1], [11, 23], [33, 38]])
        """
        close = self.diff_matrix[:, 0]
        length = close.shape[0]
        output_list = []
        cur_accum_change = 0.0
        lowest_accum_change = -10000000.00
        for i in range(length):
            cur_accum_change += close[i]
            if cur_accum_change > 0:
                if len(output_list) != 0 and output_list[-1][1] == -1:
                    output_list[-1][1] = i
                cur_accum_change = 0.0
                lowest_accum_change = 0.0
                continue
            elif output_list.__len__() == 0 or output_list[-1][1] != -1:
                output_list.append([i, -1])
            else:
                pass

            if cur_accum_change < lowest_accum_change:
                if cur_accum_change < (tolerance_percent * lowest_accum_change) or i == length-1:
                    output_list[-1][1] = i
                    cur_accum_change = 0.0
                    lowest_accum_change = -10000000.00
            elif i == length - 1:
                if output_list[-1][0] == i:
                    output_list.pop()
                else:
                    output_list[-1][1] = i
            else:
                lowest_accum_change = cur_accum_change
        return output_list

    def get_klines(self):
        return self.client.get_historical_klines(self.coin_market, self.get_interval(), self.from_time, self.to_time)

    def get_interval(self):
        """
        0    self.client.KLINE_INTERVAL_1MONTH,
        1    self.client.KLINE_INTERVAL_1WEEK,
        2    self.client.KLINE_INTERVAL_3DAY,
        3    self.client.KLINE_INTERVAL_1DAY,
        4    self.client.KLINE_INTERVAL_12HOUR,
        5    self.client.KLINE_INTERVAL_8HOUR,
        6    self.client.KLINE_INTERVAL_6HOUR,
        7    self.client.KLINE_INTERVAL_4HOUR,
        8    self.client.KLINE_INTERVAL_2HOUR,
        9    self.client.KLINE_INTERVAL_1HOUR,
        10   self.client.KLINE_INTERVAL_30MINUTE,
        11   self.client.KLINE_INTERVAL_15MINUTE,
        12   self.client.KLINE_INTERVAL_5MINUTE,
        13   self.client.KLINE_INTERVAL_3MINUTE,
        14   self.client.KLINE_INTERVAL_1MINUTE
        :return:
        """
        interval_list = [self.client.KLINE_INTERVAL_1MONTH,
                         self.client.KLINE_INTERVAL_1WEEK,
                         self.client.KLINE_INTERVAL_3DAY,
                         self.client.KLINE_INTERVAL_1DAY,
                         self.client.KLINE_INTERVAL_12HOUR,
                         self.client.KLINE_INTERVAL_8HOUR,
                         self.client.KLINE_INTERVAL_6HOUR,
                         self.client.KLINE_INTERVAL_4HOUR,
                         self.client.KLINE_INTERVAL_2HOUR,
                         self.client.KLINE_INTERVAL_1HOUR,
                         self.client.KLINE_INTERVAL_30MINUTE,
                         self.client.KLINE_INTERVAL_15MINUTE,
                         self.client.KLINE_INTERVAL_5MINUTE,
                         self.client.KLINE_INTERVAL_3MINUTE,
                         self.client.KLINE_INTERVAL_1MINUTE]
        return interval_list[self.interval_idx]
