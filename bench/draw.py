#!/usr/bin/env python3

from copy import deepcopy
import matplotlib.pyplot as plt
import numpy as np
import json
import sys
import pandas as pd


def print_plots(data, title, fname):
    ax = data.plot(kind='bar', figsize=(16, 9), fontsize=12)
    ax.set_xlabel("\n" + title, fontsize=12)
    ax.set_ylabel("Nanoseconds", fontsize=12)
    plt.yscale("log")
    plt.tick_params(
    axis='x',
    which='both',
    bottom=False,
    top=False,
    labelbottom=False)
    # plt.savefig("graphics/" + fname[:-5] + '.png')
    plt.show()
   

def print_plots_from_file(file):
    with open(file) as chart1:
        kek = 0
        data_map = json.load(chart1)
        for_draw = deepcopy(data_map)
        for_draw.pop("already_in", None)
        for_draw.pop("read_percent", None)
        for_draw.pop("total_queries", None)
        df = pd.DataFrame(for_draw, index=[0])
        print_plots(df, "already_in = " + str(data_map["already_in"]) + " keys\nread_percent = " + str(data_map["read_percent"]) 
        + "%\ntotal_queries = " + str(data_map["total_queries"]), file)
for file in sys.argv[1:]:
    print("file = " + file)
    print_plots_from_file(file)
