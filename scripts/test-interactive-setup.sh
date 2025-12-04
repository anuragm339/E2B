#!/bin/bash
# Test script to verify interactive setup works with default values

echo "Testing setup-interactive.sh with default values..."
echo ""

# Set default values and pipe to the interactive script
{
  echo ""  # Provider path (use default)
  echo ""  # Consumer path (use default)
  echo ""  # Monitoring path (use default)
  echo ""  # Cloud server path (use default)
  echo ""  # CA cert path (use default)
  echo "y" # Confirm
} | ./setup-interactive.sh

echo ""
echo "Test completed. Check output above for any errors."
