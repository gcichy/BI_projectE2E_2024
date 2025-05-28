## Data Science Project Template

You can use this template to structure your Python data science projects. It is based on [Cookie Cutter Data Science](https://drivendata.github.io/cookiecutter-data-science/).

import matplotlib.colors as mcolors
import re

def is_valid_plotly_color(color):
    # Try matplotlib
    try:
        mcolors.to_rgba(color)
        return True
    except ValueError:
        pass

    # Check for CSS rgb/rgba
    if isinstance(color, str) and re.match(r'^rgba?\((\s*\d+\s*,){2,3}\s*[\d.]+\s*\)$', color):
        return True

    # Check for hsl
    if isinstance(color, str) and re.match(r'^hsl\(\s*\d+\s*,\s*\d+%?\s*,\s*\d+%?\s*\)$', color):
        return True

    return False

# Example usage
color_palette = ['red', '#FF5733', 'rgb(255,0,0)', 'rgba(255,0,0,0.5)', 'hsl(0,100%,50%)', 'invalid']
valid_colors = [c for c in color_palette if is_valid_plotly_color(c)]

print("Valid colors:", valid_colors)
