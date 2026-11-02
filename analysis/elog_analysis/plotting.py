"""One look for every experiment figure (PNG, light background, for the report).

Colours are the first slots of a validated categorical palette, always assigned in this
order, so a series keeps its colour across figures. Charts use at most three series and
one y-axis; a figure with more splits into panels.
"""

import matplotlib

matplotlib.use("Agg")  # files only, no display
import matplotlib.pyplot as plt  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]  # blue, orange, aqua
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
MARKER = "#52514e"  # fault and phase markers: neutral, never a series colour

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "figure.figsize": (7.5, 3.6), "figure.dpi": 120, "savefig.dpi": 150,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "text.color": INK, "axes.labelcolor": INK_SECONDARY,
    "xtick.color": INK_SECONDARY, "ytick.color": INK_SECONDARY,
    "axes.edgecolor": GRID, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.grid.axis": "y", "axes.axisbelow": True, "grid.color": GRID,
    "grid.linewidth": 0.8,
    "lines.linewidth": 2, "lines.markersize": 8, "patch.linewidth": 0,
    "legend.frameon": False, "axes.prop_cycle": matplotlib.cycler(color=SERIES),
})


def figure(title, xlabel, ylabel, ncols=1):
    fig, axes = plt.subplots(1, ncols, figsize=(7.5 if ncols == 1 else 4.2 * ncols, 3.6),
                             squeeze=False)
    for ax in axes[0]:
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
    fig.suptitle(title, x=0.01, ha="left", fontweight="bold", fontsize=11)
    return fig, (axes[0][0] if ncols == 1 else list(axes[0]))


def mark(ax, x, label):
    """A labelled vertical marker, e.g. the moment a fault was injected."""
    ax.axvline(x, color=MARKER, linewidth=1, linestyle="--")
    ax.annotate(label, (x, 1), xycoords=("data", "axes fraction"), xytext=(3, -12),
                textcoords="offset points", color=INK_SECONDARY, fontsize=9)


def save(fig, path):
    for ax in fig.axes:
        if len(ax.get_lines()) + len(ax.containers) > 1 and ax.get_legend_handles_labels()[1]:
            ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
