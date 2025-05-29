import pandas as pd
class Line_zhongshu:
    def __init__(self, lines, begin_num, overlap_price_high, overlap_price_low):
        # print(lines.shape)
        self.zs_type = lines.iloc[1]['Line_Type']
        self.begin_num = begin_num
        self.end_num = begin_num + 2
        self.overlap_start_date = lines.iloc[0]['startDate']
        self.overlap_end_date = lines.iloc[-1]['endDate']
        self.overlap_price_high = overlap_price_high
        self.overlap_price_low = overlap_price_low
        self.inside_lines = lines

    def get_inside_lines_num(self):
        return len(self.inside_lines)

    def add_to_inside_lines(self, line):
        self.inside_lines = pd.concat([self.inside_lines, line], ignore_index=True)
        self.end_num += 1

    def remove_last_line(self):
        self.inside_lines = self.inside_lines.iloc[:-1]
        self.end_num -= 1
    def same_level_and_cross_level_decomposition(self):
        # temp = self.inside_lines['Line_Type'] == self.zs_type
        list_index = [4, 6, 8]
        list_index = [it - 1 for it in list_index]
        if self.zs_type == 'down':
            rows = self.inside_lines.iloc[list_index]
            min_row = rows['endPrice'].idxmin() # 找出endPrice最小的那一行
            # print(min_row)
            self.inside_lines = self.inside_lines[:min_row]
            self.end_num = self.begin_num + len(self.inside_lines) - 1
            self.overlap_end_date = self.inside_lines.iloc[-1]['endDate']
            return min_row + self.begin_num - 1
        if self.zs_type == 'up':
            rows = self.inside_lines.iloc[list_index]
            max_row = rows['endPrice'].idxmax()  # 找出endPrice最小的那一行
            # print(max_row)
            self.inside_lines = self.inside_lines[:max_row]
            self.end_num = self.begin_num + len(self.inside_lines) - 1
            self.overlap_end_date = self.inside_lines.iloc[-1]['endDate']
            return max_row + self.begin_num - 1
