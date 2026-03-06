#!/usr/bin/env python3
"""
Emergency CSV Fixer for ParaAI Queue Director
==============================================

This script fixes the CSV parsing error at line 4722.

Run this on your server where the app is running:
    python emergency_csv_fix.py
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime


def find_csv_file():
    """Locate the state CSV file."""
    candidates = [
        Path('work_director/state_chunks.csv'),
        Path('repo_git_temp/new_tracking_processamento.csv'),
        Path('/home/user/app/work_director/state_chunks.csv'),
        Path('/home/user/app/repo_git_temp/new_tracking_processamento.csv'),
    ]
    
    for path in candidates:
        if path.exists():
            return path
    
    return None


def diagnose_csv(csv_path):
    """Find malformed lines in the CSV."""
    print(f"\n🔍 Diagnosing: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        header = f.readline().strip()
        num_cols = len(header.split(','))
        
        print(f"✓ Header: {num_cols} columns")
        print(f"  {header[:100]}...\n")
        
        issues = []
        for line_num, line in enumerate(f, start=2):
            if not line.strip():
                continue
            
            # Simple field count (doesn't handle quoted commas properly)
            field_count = len(line.split(','))
            
            if field_count != num_cols:
                issues.append((line_num, field_count, line))
                
                if len(issues) <= 10:
                    print(f"❌ Line {line_num}: {field_count} fields (expected {num_cols})")
                    # Show the line with visible quotes and commas
                    preview = line.strip()[:150]
                    print(f"   {preview}...")
                    print()
        
        return num_cols, issues


def fix_csv_robust(csv_path, backup_path):
    """Fix CSV using Python's csv module for proper handling."""
    import csv
    
    print(f"\n🔧 Repairing with robust CSV parser...")
    
    fixed_lines = 0
    skipped_lines = 0
    
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as fin:
        reader = csv.reader(fin)
        
        # Read header
        try:
            header = next(reader)
            num_cols = len(header)
        except StopIteration:
            print("❌ Empty CSV file!")
            return False
        
        # Create temporary output
        temp_path = csv_path.with_suffix('.tmp')
        
        with open(temp_path, 'w', encoding='utf-8', newline='') as fout:
            writer = csv.writer(fout)
            writer.writerow(header)
            
            for line_num, row in enumerate(reader, start=2):
                if len(row) == num_cols:
                    # Perfect row
                    writer.writerow(row)
                elif len(row) > num_cols:
                    # Too many fields - truncate
                    print(f"✂️  Line {line_num}: truncating {len(row)} → {num_cols} fields")
                    writer.writerow(row[:num_cols])
                    fixed_lines += 1
                elif len(row) < num_cols:
                    # Too few fields - pad with empty strings
                    print(f"📝 Line {line_num}: padding {len(row)} → {num_cols} fields")
                    padded_row = row + [''] * (num_cols - len(row))
                    writer.writerow(padded_row)
                    fixed_lines += 1
                else:
                    # This shouldn't happen but just in case
                    print(f"⚠️  Line {line_num}: skipping unusual row")
                    skipped_lines += 1
    
    # Replace original with fixed version
    shutil.move(str(temp_path), str(csv_path))
    
    print(f"\n✅ Repair complete!")
    print(f"   Fixed: {fixed_lines} lines")
    print(f"   Skipped: {skipped_lines} lines")
    
    return True


def main():
    print("=" * 60)
    print("🚑 Emergency CSV Fixer for ParaAI Queue Director")
    print("=" * 60)
    
    # Find the CSV file
    csv_path = find_csv_file()
    
    if csv_path is None:
        print("\n❌ Could not find CSV file!")
        print("\nSearched locations:")
        print("  • work_director/state_chunks.csv")
        print("  • repo_git_temp/new_tracking_processamento.csv")
        print("\nPlease run this script from the app directory or specify the path:")
        print("  python emergency_csv_fix.py <path_to_csv>")
        sys.exit(1)
    
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    
    if not csv_path.exists():
        print(f"\n❌ File not found: {csv_path}")
        sys.exit(1)
    
    # Diagnose
    num_cols, issues = diagnose_csv(csv_path)
    
    if not issues:
        print("✅ No issues found! CSV is clean.")
        return
    
    print(f"\n⚠️  Found {len(issues)} problematic lines")
    
    # Create backup
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = csv_path.with_suffix(f'.backup_{timestamp}')
    
    print(f"\n💾 Creating backup: {backup_path}")
    shutil.copy2(csv_path, backup_path)
    
    # Fix
    print("\nProceed with repair? (y/n): ", end='')
    response = input().strip().lower()
    
    if response != 'y':
        print("❌ Aborted")
        return
    
    if fix_csv_robust(csv_path, backup_path):
        print(f"\n✅ CSV repaired successfully!")
        print(f"\n📋 Next steps:")
        print(f"   1. Restart your application")
        print(f"   2. Check logs for successful startup")
        print(f"   3. If issues persist, restore from: {backup_path}")
    else:
        print(f"\n❌ Repair failed. Original file unchanged.")
        print(f"   Backup is at: {backup_path}")


if __name__ == '__main__':
    main()
