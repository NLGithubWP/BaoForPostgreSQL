import pandas as pd
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
import string
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np
import statistics


plt.rcParams["font.size"] = 16
SHOW_RG = False

with open("pg_run.txt") as f:
    data = f.read().split("\n")[2:]
data = [x.split(" ") for x in data if len(x) > 1 and (x[0] in string.digits or x[0] == "x")]

data = [(x[0], x[1], float(x[2]), x[3], float(x[4])) for x in data]
pg_data = data
pg_times = np.array([x[2] for x in pg_data])
pg_times -= np.min(pg_times)
pg_times /= 60 # this is convert into minutes


def read_bao_data(fp):
    with open(fp) as f:
        data = f.read().split("\n")[2:]

    training_times = []
    training_end_time = []
    for idx in range(len(data)):
        if data[idx].strip().startswith("Initial input channels"): # this is begin of training.
            prev_line = data[idx - 1].split(" ")
            if prev_line[0] == "Retry":
                continue
            training_times.append(float(prev_line[2]))

    for idx in range(len(data)):
        if data[idx].strip().startswith("New model had no regressions."):
            next_line = data[idx + 1].split(" ")
            training_end_time.append(float(next_line[2]))

    training_times = np.array(training_times)
    training_end_time = np.array(training_end_time)

    data = [x.split(" ") for x in data if len(x) > 1 and (x[0] in string.digits or x[0] == "x")]
    data = [(x[0], x[1], float(x[2]), x[3], float(x[4])) for x in data]
    bao_data = data

    bao_times = np.array([x[2] for x in bao_data])
    training_times -= np.min(bao_times)
    training_end_time -= np.min(bao_times)
    bao_times -= np.min(bao_times)

    bao_times /= 60
    training_times /= 60
    training_end_time /= 60
    return bao_data, bao_times, training_times, training_end_time


bao_data, bao_times, training_times, training_end_time = read_bao_data("bao_run.txt")
if SHOW_RG:
    bao_rb_data, bao_rb_times, training_rb_times, training_end_time = read_bao_data("bao_with_regblock.txt")

queries_complete = np.arange(0, len(pg_times))

fig, ax = plt.subplots(1, 1, constrained_layout=True)

train_y = []
train_y_tain_end = []
train_rb_y = []
for tt in training_times:
    idx = np.searchsorted(bao_times, tt)
    train_y.append(idx)

for tt in training_end_time:
    idx = np.searchsorted(bao_times, tt)
    train_y_tain_end.append(idx)


if SHOW_RG:
    for tt in training_rb_times:
        idx = np.searchsorted(bao_rb_times, tt)
        train_rb_y.append(idx)

plt.scatter(training_times, train_y, s=30, color="red", label="Training Begin")
plt.scatter(training_end_time, train_y_tain_end, s=30, color="blue", label="Training End")


ax.plot(pg_times, queries_complete, label="PostgreSQL Optimizer", lw=3)
ax.plot(bao_times, queries_complete, label="Learned Query Optimizer", lw=3)

if SHOW_RG:
    plt.scatter(training_rb_times, train_rb_y, s=45, color="red")
    ax.plot(bao_rb_times, queries_complete, label="Bao (w/ exploration)", lw=3)

ax.set_xlabel("Time (m)")
ax.set_ylabel("Queries complete")

ax.grid(linestyle="--", linewidth=1)
ax.legend()
fig.savefig("queries_vs_time.jpg")


# Creating a single figure
fig_bar, ax_bar = plt.subplots(figsize=(8, 6))

# Bar positions
bao_bar_pos = [0.4]
pg_bar_pos = [1.2]

# Choosing softer colors for the bars
soft_green = "#9ACD32"  # A soft green color
soft_orange = "#FFA07A"  # A soft orange color

# Plotting the bars with softer colors
ax_bar.bar(bao_bar_pos, max(bao_times), width=0.4, label='Learned Query Execution', color=soft_green)
ax_bar.bar(pg_bar_pos, max(pg_times), width=0.4, label='PG Query Execution', color=soft_orange)


# Removing the red and blue lines and adding filled color between those lines
i = 0
for t_start, t_end in zip(training_times, training_end_time):
    print(t_end - t_start)
    if i == 0:
        ax_bar.fill_between([0.2, 0.6], t_start, t_end, color='red', alpha=0.5, linewidth=0, label='Training')
        i = 1
    else:
        ax_bar.fill_between([0.2, 0.6], t_start, t_end, color='red', alpha=0.5, linewidth=0)

# Setting chart details
ax_bar.set_xticks([0.4, 1.2])
ax_bar.set_xticklabels(['Learned Query Optimizer', 'PostgreSQL Optimizer'], rotation=5)
ax_bar.set_ylabel('Time (min)')
ax_bar.legend(loc='upper left')
ax_bar.grid()

# Display the plot
fig_bar.savefig("bar.jpg")


all_pg_times = sorted([x[4] for x in pg_data])
all_bao_times = sorted([x[4] for x in bao_data])

if SHOW_RG:
    all_bao_rb_times = sorted([x[4] for x in bao_rb_data])

fig, axes = plt.subplots(1, 2, figsize=(10, 4), constrained_layout=True)

ax = axes[0]
ax.plot(np.linspace(0, 1, len(all_pg_times)), all_pg_times, lw=3, label="PostgreSQL Optimizer")
ax.plot(np.linspace(0, 1, len(all_pg_times)), all_bao_times, lw=3, label="Learned Query Optimizer")

if SHOW_RG:

    ax.plot(np.linspace(0, 1, len(all_pg_times)), all_bao_rb_times, lw=3, label="Bao (w/ exploration)")

ax.grid(linestyle="--", linewidth=1)
ax.set_xlabel("Proportion of Queries")
ax.set_ylabel("Max Latency (s)")
ax.set_title("Query Latency CDF")
ax.legend()
# ax.set_yscale("log")


ax = axes[1]
ax.plot(np.linspace(0, 1, len(all_pg_times)), all_pg_times, lw=3, label="PostgreSQL Optimizer")
ax.plot(np.linspace(0, 1, len(all_pg_times)), all_bao_times, lw=3, label="Learned Query Optimizer")

if SHOW_RG:
    ax.plot(np.linspace(0, 1, len(all_pg_times)), all_bao_rb_times, lw=3, label="Bao (w/ exploration)")

ax.grid(linestyle="--", linewidth=1)
ax.set_xlabel("Proportion of Queries")
ax.set_ylabel("Max Latency (s)")
ax.set_title("Query Latency CDF (log scale)")
ax.legend()
ax.set_yscale("log")
fig.savefig("cdf.jpg")


# get the last PG time for each query
pg_query_time = {}
pg_query_num = {}
for itm in pg_data:
    if itm[3] not in pg_query_time:
        pg_query_time[itm[3]] = itm[4]
        pg_query_num[itm[3]] = 1
    else:
        pg_query_time[itm[3]] += itm[4]
        pg_query_num[itm[3]] += 1

for itm in pg_query_time:
    pg_query_time[itm] = pg_query_time[itm]/pg_query_num[itm]


# get each Bao time
bao_query_times = defaultdict(list)
for itm in bao_data[50:]:
    bao_query_times[itm[3]].append(itm[4])

if SHOW_RG:
    # get each Bao time
    bao_rb_query_times = defaultdict(list)
    for itm in bao_rb_data[50:]:
        bao_rb_query_times[itm[3]].append(itm[4])

max_repeats = max(len(x) for x in bao_query_times.values())


def extract_q_number(x):
    return int(x[x.find("/q") + 2:x.find("_", x.find("/q"))])


q_order = sorted(bao_query_times.keys(), key=extract_q_number)

grid = [bao_query_times[x] for x in q_order]

if SHOW_RG:
    grid_rb = [bao_rb_query_times[x] for x in q_order]

reg_data = []
for idx, q in enumerate(q_order):
    if SHOW_RG:
        reg_data.append({"Q": f"q{extract_q_number(q)}",
                         "PG": pg_query_time[q],
                         "Bao worst": max(grid[idx]),
                         "Bao best": min(grid[idx]),
                         "Bao + E worst": max(grid_rb[idx]),
                         "Bao + E best": min(grid_rb[idx])})
    else:
        reg_data.append({"Q": f"q{extract_q_number(q)}",
                         "PG": pg_query_time[q],
                         "Bao average": statistics.mean(grid[idx]),
                         "Bao worst": max(grid[idx]),
                         "Bao best": min(grid[idx])})


def color_regression(col):
    def c_for_diff(diff):
        if diff < 2 and diff > -2:
            return "background-color: white"
        elif diff > 0.5:
            return "background-color: #f27281"
        else:
            return "background-color: #9ee3ad"

    to_r = [""]

    if SHOW_RG:
        pg, bao_worst, bao_best, bao_rg_worst, bao_rg_best = col
    else:
        pg, bao_worst, bao_best = col

    to_r.append(c_for_diff(bao_worst - pg))
    to_r.append(c_for_diff(bao_best - pg))

    if SHOW_RG:
        to_r.append(c_for_diff(bao_rg_worst - pg))
        to_r.append(c_for_diff(bao_rg_best - pg))

    return to_r


reg_data = pd.DataFrame(reg_data).set_index("Q")
reg_data.style.apply(color_regression, axis=1)


# Plotting
fig, ax = plt.subplots(figsize=(16, 6))
x = np.arange(len(reg_data))  # the label locations
width = 0.35  # the width of the bars

# Plotting the bars for 'Bao average' and 'PG'
rects1 = ax.bar(x - width/2, reg_data['Bao best'], width, label='Learned Query Optimizer', color='green')
rects2 = ax.bar(x + width/2, reg_data['PG'], width, label='PostgreSQL Optimizer', color='blue')

# Add some text for labels, title, and custom x-axis tick labels, etc.
ax.set_ylabel('Query Execution Time (sec)')
ax.set_xticks(x)
ax.set_xticklabels(reg_data.index, rotation=45)
ax.legend()

ax.grid(True, zorder=0)  # This ensures that the grid is at the back.
ax.set_axisbelow(True)   # This ensures that the axis ticks and labels are above the grid lines.


ax.set_yscale("log")
fig.savefig("each_query.jpg")
