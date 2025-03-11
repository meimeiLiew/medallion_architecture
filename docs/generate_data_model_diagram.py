#!/usr/bin/env python3
"""
Generate a data model diagram for the medallion architecture.
This script creates a visualization of the data models in each layer
(Bronze, Silver, Gold) and their relationships.
"""

from PIL import Image, ImageDraw, ImageFont
import os

# Create directory if it doesn't exist
os.makedirs('docs', exist_ok=True)

# Image dimensions
WIDTH = 1500
HEIGHT = 1200
PADDING = 50

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
BLUE = (66, 133, 244)  # Google Blue
BRONZE = (205, 127, 50)
SILVER = (192, 192, 192)
GOLD = (255, 215, 0)
LIGHT_GRAY = (240, 240, 240)
DARK_GRAY = (100, 100, 100)

# Create a new image with white background
image = Image.new('RGB', (WIDTH, HEIGHT), WHITE)
draw = ImageDraw.Draw(image)

# Try to load fonts, fall back to default if not available
try:
    title_font = ImageFont.truetype("Arial Bold.ttf", 36)
    header_font = ImageFont.truetype("Arial Bold.ttf", 24)
    table_font = ImageFont.truetype("Arial Bold.ttf", 18)
    field_font = ImageFont.truetype("Arial.ttf", 14)
    regular_font = ImageFont.truetype("Arial.ttf", 16)
except IOError:
    # Fall back to default font
    title_font = ImageFont.load_default()
    header_font = ImageFont.load_default()
    table_font = ImageFont.load_default()
    field_font = ImageFont.load_default()
    regular_font = ImageFont.load_default()

# Draw title
title = "Medallion Architecture Data Model"
draw.text((WIDTH // 2, PADDING), title, fill=BLACK, font=title_font, anchor="mt")

# Layer dimensions
layer_width = (WIDTH - (PADDING * 4)) // 3
layer_height = HEIGHT - (PADDING * 3) - 100
layer_y = PADDING * 2 + 50

# Function to draw a table
def draw_table(x, y, width, height, title, fields, color, border_color=BLACK):
    # Draw table background
    draw.rectangle(
        [(x, y), (x + width, y + height)],
        outline=border_color,
        fill=color,
        width=2
    )
    
    # Draw table title
    title_height = 40
    draw.rectangle(
        [(x, y), (x + width, y + title_height)],
        outline=border_color,
        fill=border_color,
        width=2
    )
    draw.text(
        (x + width // 2, y + title_height // 2),
        title,
        fill=WHITE,
        font=table_font,
        anchor="mm"
    )
    
    # Draw fields
    field_y = y + title_height + 10
    for field in fields:
        draw.text(
            (x + 15, field_y),
            field,
            fill=BLACK,
            font=field_font,
            anchor="lm"
        )
        field_y += 25

# Function to draw an arrow
def draw_arrow(start_x, start_y, end_x, end_y, color=BLUE, width=2):
    draw.line(
        [(start_x, start_y), (end_x, end_y)],
        fill=color,
        width=width
    )
    # Arrow head
    arrow_size = 10
    if end_x > start_x:  # Right arrow
        draw.polygon(
            [(end_x, end_y), (end_x - arrow_size, end_y - arrow_size // 2), 
             (end_x - arrow_size, end_y + arrow_size // 2)],
            fill=color
        )
    elif end_x < start_x:  # Left arrow
        draw.polygon(
            [(end_x, end_y), (end_x + arrow_size, end_y - arrow_size // 2), 
             (end_x + arrow_size, end_y + arrow_size // 2)],
            fill=color
        )
    elif end_y > start_y:  # Down arrow
        draw.polygon(
            [(end_x, end_y), (end_x - arrow_size // 2, end_y - arrow_size), 
             (end_x + arrow_size // 2, end_y - arrow_size)],
            fill=color
        )
    else:  # Up arrow
        draw.polygon(
            [(end_x, end_y), (end_x - arrow_size // 2, end_y + arrow_size), 
             (end_x + arrow_size // 2, end_y + arrow_size)],
            fill=color
        )

# Draw the three layers
# Bronze Layer
bronze_x = PADDING
draw.rectangle(
    [(bronze_x, layer_y), (bronze_x + layer_width, layer_y + layer_height)],
    outline=BLACK,
    fill=LIGHT_GRAY,
    width=2
)
draw.text(
    (bronze_x + layer_width // 2, layer_y + 30),
    "Bronze Layer",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)

# Silver Layer
silver_x = bronze_x + layer_width + PADDING
draw.rectangle(
    [(silver_x, layer_y), (silver_x + layer_width, layer_y + layer_height)],
    outline=BLACK,
    fill=LIGHT_GRAY,
    width=2
)
draw.text(
    (silver_x + layer_width // 2, layer_y + 30),
    "Silver Layer",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)

# Gold Layer
gold_x = silver_x + layer_width + PADDING
draw.rectangle(
    [(gold_x, layer_y), (gold_x + layer_width, layer_y + layer_height)],
    outline=BLACK,
    fill=LIGHT_GRAY,
    width=2
)
draw.text(
    (gold_x + layer_width // 2, layer_y + 30),
    "Gold Layer",
    fill=BLACK,
    font=header_font,
    anchor="mm"
)

# Define tables for each layer
# Bronze tables
bronze_table_width = layer_width - 40
bronze_table_height = 180

# Contracts table
contracts_bronze_y = layer_y + 80
draw_table(
    bronze_x + 20, 
    contracts_bronze_y, 
    bronze_table_width, 
    bronze_table_height, 
    "contracts_bronze", 
    [
        "contract_id: STRING",
        "contract_name: STRING",
        "client_name: STRING",
        "start_date: STRING",
        "end_date: STRING",
        "contract_value: STRING",
        "contract_status: STRING"
    ], 
    BRONZE
)

# Budgets table
budgets_bronze_y = contracts_bronze_y + bronze_table_height + 30
draw_table(
    bronze_x + 20, 
    budgets_bronze_y, 
    bronze_table_width, 
    bronze_table_height, 
    "budgets_bronze", 
    [
        "budget_id: STRING",
        "contract_id: STRING",
        "budget_name: STRING",
        "budget_amount: STRING",
        "fiscal_year: STRING",
        "department: STRING",
        "created_at: STRING"
    ], 
    BRONZE
)

# Change Orders table
change_orders_bronze_y = budgets_bronze_y + bronze_table_height + 30
draw_table(
    bronze_x + 20, 
    change_orders_bronze_y, 
    bronze_table_width, 
    bronze_table_height, 
    "change_orders_bronze", 
    [
        "change_order_id: STRING",
        "contract_id: STRING",
        "change_order_name: STRING",
        "change_amount: STRING",
        "approval_date: STRING",
        "status: STRING",
        "notes: STRING"
    ], 
    BRONZE
)

# Silver tables
silver_table_width = layer_width - 40
silver_table_height = 180

# Contracts table
contracts_silver_y = layer_y + 80
draw_table(
    silver_x + 20, 
    contracts_silver_y, 
    silver_table_width, 
    silver_table_height, 
    "contracts_silver", 
    [
        "contract_id: INTEGER",
        "contract_name: STRING",
        "client_name: STRING",
        "start_date: DATE",
        "end_date: DATE",
        "contract_value: NUMERIC",
        "contract_status: STRING"
    ], 
    SILVER
)

# Budgets table
budgets_silver_y = contracts_silver_y + silver_table_height + 30
draw_table(
    silver_x + 20, 
    budgets_silver_y, 
    silver_table_width, 
    silver_table_height, 
    "budgets_silver", 
    [
        "budget_id: INTEGER",
        "contract_id: INTEGER",
        "budget_name: STRING",
        "budget_amount: NUMERIC",
        "fiscal_year: INTEGER",
        "department: STRING",
        "created_at: TIMESTAMP"
    ], 
    SILVER
)

# Change Orders table
change_orders_silver_y = budgets_silver_y + silver_table_height + 30
draw_table(
    silver_x + 20, 
    change_orders_silver_y, 
    silver_table_width, 
    silver_table_height, 
    "change_orders_silver", 
    [
        "change_order_id: INTEGER",
        "contract_id: INTEGER",
        "change_order_name: STRING",
        "change_amount: NUMERIC",
        "approval_date: DATE",
        "status: STRING",
        "notes: STRING"
    ], 
    SILVER
)

# Gold tables
gold_table_width = layer_width - 40
gold_table_height = 200

# Contract Analytics table
contract_analytics_y = layer_y + 80
draw_table(
    gold_x + 20, 
    contract_analytics_y, 
    gold_table_width, 
    gold_table_height, 
    "contract_analytics", 
    [
        "contract_id: INTEGER",
        "contract_name: STRING",
        "client_name: STRING",
        "original_value: NUMERIC",
        "current_value: NUMERIC",
        "total_change_orders: INTEGER",
        "change_order_value: NUMERIC",
        "budget_allocated: NUMERIC",
        "duration_days: INTEGER"
    ], 
    GOLD
)

# Project Analytics table
project_analytics_y = contract_analytics_y + gold_table_height + 50
draw_table(
    gold_x + 20, 
    project_analytics_y, 
    gold_table_width, 
    gold_table_height + 50, 
    "project_analytics", 
    [
        "client_name: STRING",
        "total_contracts: INTEGER",
        "active_contracts: INTEGER",
        "completed_contracts: INTEGER",
        "total_contract_value: NUMERIC",
        "total_budget_allocated: NUMERIC",
        "avg_contract_value: NUMERIC",
        "avg_change_order_pct: NUMERIC",
        "fiscal_year: INTEGER",
        "department: STRING"
    ], 
    GOLD
)

# Draw arrows between tables
# Bronze to Silver
# Contracts
draw_arrow(
    bronze_x + bronze_table_width + 20,
    contracts_bronze_y + bronze_table_height // 2,
    silver_x + 20,
    contracts_silver_y + silver_table_height // 2
)

# Budgets
draw_arrow(
    bronze_x + bronze_table_width + 20,
    budgets_bronze_y + bronze_table_height // 2,
    silver_x + 20,
    budgets_silver_y + silver_table_height // 2
)

# Change Orders
draw_arrow(
    bronze_x + bronze_table_width + 20,
    change_orders_bronze_y + bronze_table_height // 2,
    silver_x + 20,
    change_orders_silver_y + silver_table_height // 2
)

# Silver to Gold
# Contracts to Contract Analytics
draw_arrow(
    silver_x + silver_table_width + 20,
    contracts_silver_y + silver_table_height // 2,
    gold_x + 20,
    contract_analytics_y + gold_table_height // 2
)

# Budgets to Contract Analytics
draw_arrow(
    silver_x + silver_table_width + 20,
    budgets_silver_y + silver_table_height // 2,
    gold_x + 20,
    contract_analytics_y + gold_table_height // 2 + 30
)

# Change Orders to Contract Analytics
draw_arrow(
    silver_x + silver_table_width + 20,
    change_orders_silver_y + silver_table_height // 2,
    gold_x + 20,
    contract_analytics_y + gold_table_height // 2 + 60
)

# All Silver tables to Project Analytics
draw_arrow(
    silver_x + silver_table_width + 20,
    contracts_silver_y + silver_table_height + 50,
    gold_x + 20,
    project_analytics_y + gold_table_height // 2
)

# Add legend
legend_y = HEIGHT - 80
legend_x = WIDTH // 2 - 300

# Bronze
draw.rectangle(
    [(legend_x, legend_y), (legend_x + 30, legend_y + 20)],
    outline=BLACK,
    fill=BRONZE,
    width=1
)
draw.text(
    (legend_x + 40, legend_y + 10),
    "Bronze Layer (Raw Data)",
    fill=BLACK,
    font=regular_font,
    anchor="lm"
)

# Silver
draw.rectangle(
    [(legend_x + 250, legend_y), (legend_x + 280, legend_y + 20)],
    outline=BLACK,
    fill=SILVER,
    width=1
)
draw.text(
    (legend_x + 290, legend_y + 10),
    "Silver Layer (Cleaned Data)",
    fill=BLACK,
    font=regular_font,
    anchor="lm"
)

# Gold
draw.rectangle(
    [(legend_x + 550, legend_y), (legend_x + 580, legend_y + 20)],
    outline=BLACK,
    fill=GOLD,
    width=1
)
draw.text(
    (legend_x + 590, legend_y + 10),
    "Gold Layer (Analytics Ready)",
    fill=BLACK,
    font=regular_font,
    anchor="lm"
)

# Save the image
image.save('docs/data_model_diagram.png')
print("Data model diagram generated: docs/data_model_diagram.png") 