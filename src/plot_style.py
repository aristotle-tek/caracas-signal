from __future__ import annotations

from typing import Any
import matplotlib as mpl


def plot_rcparams() -> dict[str, Any]:
    """
    Matplotlib rcParams intended to visually match the LaTeX manuscript (Palatino/mathpazo-ish).

    Notes:
    - prefer figure titles/captions in LaTeX; keep plot titles off by default in scripts.
    - Font fallbacks are included for portability across environments.
    """

    return {
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": False,
        "grid.alpha": 0.0,
        "font.family": "serif",
        "font.serif": [
            "Palatino",
            "Palatino Linotype",
            "Book Antiqua",
            "URW Palladio L",
            "Nimbus Roman",
            "Times New Roman",
            "DejaVu Serif",
        ],
        "mathtext.fontset": "custom",
        "mathtext.rm": "Palatino",
        "mathtext.it": "Palatino:italic",
        "mathtext.bf": "Palatino:bold",
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        # Colorblind-friendly cycle (Tableau 10 modified)
        # Blue, Orange, Green, Red, Purple, Brown, Pink, Gray, Olive, Cyan
        "axes.prop_cycle": mpl.cycler(color=[
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", 
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
        ]),
    }


def apply_plot_style() -> None:
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    mpl.rcParams.update(plot_rcparams())
