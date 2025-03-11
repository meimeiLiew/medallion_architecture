#!/usr/bin/env python3
"""
Generate a medallion architecture diagram using Pillow.
This script creates a simple visualization of the medallion architecture
with Bronze, Silver, and Gold layers.
"""

from PIL import Image, ImageDraw, ImageFont
import os

# Create directory if it doesn't exist
os.makedirs('docs', exist_ok=True)

# Image dimensions
WIDTH = 1200
HEIGHT = 800
PADDING = 50

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
BLUE = (66, 133, 244)  # Google Blue
BRONZE = (205, 127, 50)
SILVER = (192, 192, 192)
GOLD = (255, 215, 0)
LIGHT_GRAY = (240, 240, 240)

# Create a new image with white background
image = Image.new('RGB', (WIDTH, HEIGHT), WHITE)
draw = ImageDraw.Draw(image)

# Try to load fonts, fall back to default if not available
try:
    title_font = ImageFont.truetype("Arial Bold.ttf", 36)
    header_font = ImageFont.truetype("Arial Bold.ttf", 24)
    regular_font = ImageFont.truetype("Arial.ttf", 18)
except IOError:
    # Fall back to default font
    title_font = ImageFont.load_default()
    header_font = ImageFont.load_default()
    regular_font = ImageFont.load_default()

# Draw title
title = "Medallion Architecture Data Pipeline"
draw.text((WIDTH // 2, PADDING), title, fill=BLACK, font=title_font, anchor="mt")

# Layer dimensions
layer_width = (WIDTH - (PADDING * 4)) // 3
layer_height = HEIGHT - (PADDING * 4)
layer_y = PADDING * 2 + 50

# Draw the three layers
# Bronze Layer
bronze_x = PADDING
draw.rectangle(
    [(bronze_x, layer_y), (bronze_x + layer_width, layer_y + layer_height)],
    outline=BLACK,
    fill=BRONZE,
    width=2
)
draw.text(
    (bronze_x + layer_width // 2, layer_y + 30),
    "Bronze Layer",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)
draw.text(
    (bronze_x + layer_width // 2, layer_y + 70),
    "Raw Data",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (bronze_x + layer_width // 2, layer_y + 120),
    "• Google Cloud Storage",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (bronze_x + layer_width // 2, layer_y + 150),
    "• BigQuery Bronze Dataset",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (bronze_x + layer_width // 2, layer_y + 180),
    "• Raw Tables",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)

# Silver Layer
silver_x = bronze_x + layer_width + PADDING
draw.rectangle(
    [(silver_x, layer_y), (silver_x + layer_width, layer_y + layer_height)],
    outline=BLACK,
    fill=SILVER,
    width=2
)
draw.text(
    (silver_x + layer_width // 2, layer_y + 30),
    "Silver Layer",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)
draw.text(
    (silver_x + layer_width // 2, layer_y + 70),
    "Cleaned & Validated Data",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (silver_x + layer_width // 2, layer_y + 120),
    "• dbt Transformations",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (silver_x + layer_width // 2, layer_y + 150),
    "• BigQuery Silver Dataset",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (silver_x + layer_width // 2, layer_y + 180),
    "• Data Validation Tests",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)

# Gold Layer
gold_x = silver_x + layer_width + PADDING
draw.rectangle(
    [(gold_x, layer_y), (gold_x + layer_width, layer_y + layer_height)],
    outline=BLACK,
    fill=GOLD,
    width=2
)
draw.text(
    (gold_x + layer_width // 2, layer_y + 30),
    "Gold Layer",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)
draw.text(
    (gold_x + layer_width // 2, layer_y + 70),
    "Business-Ready Data",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (gold_x + layer_width // 2, layer_y + 120),
    "• Analytics Models",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (gold_x + layer_width // 2, layer_y + 150),
    "• BigQuery Gold Dataset",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)
draw.text(
    (gold_x + layer_width // 2, layer_y + 180),
    "• Reporting Views",
    fill=BLACK,
    font=regular_font,
    anchor="mm"
)

# Draw arrows between layers
arrow_y = layer_y + layer_height // 2
# Bronze to Silver
draw.line(
    [(bronze_x + layer_width, arrow_y), (silver_x, arrow_y)],
    fill=BLUE,
    width=3
)
# Arrow head
draw.polygon(
    [(silver_x, arrow_y), (silver_x - 15, arrow_y - 10), (silver_x - 15, arrow_y + 10)],
    fill=BLUE
)

# Silver to Gold
draw.line(
    [(silver_x + layer_width, arrow_y), (gold_x, arrow_y)],
    fill=BLUE,
    width=3
)
# Arrow head
draw.polygon(
    [(gold_x, arrow_y), (gold_x - 15, arrow_y - 10), (gold_x - 15, arrow_y + 10)],
    fill=BLUE
)

# Draw Airflow orchestration bar at the bottom
orchestration_y = layer_y + layer_height + PADDING
draw.rectangle(
    [(PADDING, orchestration_y), (WIDTH - PADDING, orchestration_y + 60)],
    outline=BLACK,
    fill=LIGHT_GRAY,
    width=2
)
draw.text(
    (WIDTH // 2, orchestration_y + 30),
    "Apache Airflow Orchestration",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)

# Save the image
image.save('docs/medallion_architecture.png')
print("Diagram generated: docs/medallion_architecture.png") 