import cv2
import numpy as np
import json
import os

def analyze_seats(image_path, processed_image_output_path):
    """
    Analyzes seat occupancy from the screenshot.
    Returns a dictionary of stats.
    """
    img = cv2.imread(image_path)
    if img is None:
        print(f"[Analyzer] Error: Image not found at {image_path}")
        return None

    output_img = img.copy()

    # Preprocessing
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 230, 255, cv2.THRESH_BINARY_INV)

    # Find Contours
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    total_seats = 0
    filled_sold = 0
    unsold_available = 0
    unsold_bestseller = 0

    # Color definitions (HSV)
    lower_green = np.array([40, 40, 40])
    upper_green = np.array([90, 255, 255])
    lower_yellow = np.array([15, 40, 40])
    upper_yellow = np.array([35, 255, 255])

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = cv2.contourArea(cnt)
        aspect_ratio = float(w) / h

        # Filter for seat-like shapes
        if area > 150 and area < 3000 and 0.7 < aspect_ratio < 1.4:
            roi_hsv = hsv[y:y+h, x:x+w]
            
            mask_green = cv2.inRange(roi_hsv, lower_green, upper_green)
            mask_yellow = cv2.inRange(roi_hsv, lower_yellow, upper_yellow)
            
            green_pixels = cv2.countNonZero(mask_green)
            yellow_pixels = cv2.countNonZero(mask_yellow)

            total_seats += 1
            pixel_threshold = 10

            if yellow_pixels > pixel_threshold and yellow_pixels > green_pixels:
                unsold_bestseller += 1
                cv2.rectangle(output_img, (x, y), (x+w, y+h), (255, 0, 0), 2) # Blue box
            elif green_pixels > pixel_threshold:
                unsold_available += 1
                cv2.rectangle(output_img, (x, y), (x+w, y+h), (0, 255, 0), 2) # Green box
            else:
                filled_sold += 1
                cv2.rectangle(output_img, (x, y), (x+w, y+h), (0, 0, 255), 2) # Red box

    # Save processed visual
    cv2.imwrite(processed_image_output_path, output_img)

    return {
        "total_seats": total_seats,
        "filled_sold": filled_sold,
        "available": unsold_available,
        "bestseller": unsold_bestseller,
        "total_unsold": unsold_available + unsold_bestseller,
        "processed_image_path": processed_image_output_path
    }