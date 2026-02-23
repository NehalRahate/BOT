# Bot Exception Handler - Automation Script

## Overview

This automation script monitors database exceptions from bot processes (Type 1 and Type 6) and automatically retries failed inventory items by updating their file status. The script runs continuously in a loop with a 10-minute interval and ensures each inventory ID is only processed once per day.

## Features

✅ **Automated Exception Monitoring**: Queries database every 10 minutes for new exceptions  
✅ **Dual Process Type Support**: Handles both process type 1 and process type 6  
✅ **Duplicate Prevention**: Tracks processed inventory IDs to avoid re-processing  
✅ **Daily Reset**: Automatically resets tracking when date changes  
✅ **Comprehensive Logging**: Both file and console logging for monitoring  
✅ **Error Handling**: Robust error handling with detailed logging  

## Requirements

- Python 3.7 or higher
- PostgreSQL database
- Required Python packages:
  - psycopg2 (PostgreSQL adapter)

## Installation

### 1. Install Python Dependencies

```bash
pip install psycopg2-binary
```

Or if you have PostgreSQL development libraries installed:

```bash
pip install psycopg2
```

### 2. Set Up Configuration

1. Copy the configuration template:
   ```bash
   cp config_template.py config.py
   ```

2. Edit `config.py` with your database credentials:
   ```python
   DATABASE_CONFIG = {
       'host': 'your_database_host',      # e.g., 'localhost' or '192.168.1.100'
       'database': 'your_database_name',  # e.g., 'bot_system'
       'user': 'your_username',           # e.g., 'postgres'
       'password': 'your_password',       # Your database password
       'port': 5432                       # Default PostgreSQL port
   }
   
   CHECK_INTERVAL = 10  # Check every 10 minutes
   ```

## Usage

### Running the Script

**Option 1: Using the version with config file (Recommended)**

```bash
python bot_exception_automator_v2.py
```

**Option 2: Using the standalone version**

Edit the database configuration directly in `bot_exception_automator.py` and run:

```bash
python bot_exception_automator.py
```

### Stopping the Script

Press `Ctrl+C` to stop the script gracefully.

## How It Works

1. **Query Exceptions**: Every 10 minutes, the script queries the database for inventory IDs with exceptions:
   ```sql
   -- For Process Type 1
   SELECT DISTINCT inventoryid 
   FROM public.tbl_botlogs 
   WHERE message LIKE '%Exception : file %' 
   AND created_time::Date = Current_Date 
   AND process_type = '1'
   
   -- For Process Type 6
   SELECT DISTINCT inventoryid 
   FROM public.tbl_botlogs 
   WHERE message LIKE '%Exception : file %' 
   AND created_time::Date = Current_Date 
   AND process_type = '6'
   ```

2. **Filter New IDs**: Compares found inventory IDs against already processed ones

3. **Update Status**: For new inventory IDs, updates the file status:
   ```sql
   UPDATE public.tbl_stginventoryuploaddata 
   SET filestatus = 0 
   WHERE inventoryid IN (...)
   ```

4. **Track Processing**: Marks inventory IDs as processed to prevent duplicate updates

5. **Reset Daily**: Automatically clears the processed list when the date changes

## Log Files

The script creates a log file `bot_exception_handler.log` that contains:
- Timestamp of each operation
- Number of exceptions found
- Inventory IDs being processed
- Update confirmation
- Any errors encountered

### Sample Log Output

```
2025-02-12 10:00:01 - INFO - Bot Exception Handler started - Running every 10 minutes
2025-02-12 10:00:01 - INFO - Press Ctrl+C to stop
2025-02-12 10:00:01 - INFO - ================================================================================
2025-02-12 10:00:01 - INFO - Starting exception processing cycle
2025-02-12 10:00:01 - INFO - Found 3 inventory IDs with exceptions for process type 1
2025-02-12 10:00:01 - INFO - Found 2 inventory IDs with exceptions for process type 6
2025-02-12 10:00:01 - INFO - New inventory IDs to process: 5
2025-02-12 10:00:01 - INFO - Inventory IDs: 12345, 12346, 12347, 12348, 12349
2025-02-12 10:00:01 - INFO - Updated 5 inventory records to filestatus=0
2025-02-12 10:00:01 - INFO - Total processed inventory IDs today: 5
2025-02-12 10:00:01 - INFO - Exception processing cycle completed
2025-02-12 10:00:01 - INFO - ================================================================================
2025-02-12 10:00:01 - INFO - Waiting 10 minutes until next check...
```

## Running as a Background Service

### Linux/Unix (using systemd)

1. Create a service file `/etc/systemd/system/bot-exception-handler.service`:

```ini
[Unit]
Description=Bot Exception Handler Service
After=network.target postgresql.service

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/script/directory
ExecStart=/usr/bin/python3 /path/to/script/bot_exception_automator_v2.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

2. Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable bot-exception-handler
sudo systemctl start bot-exception-handler
```

3. Check status:

```bash
sudo systemctl status bot-exception-handler
```

### Windows (using Task Scheduler)

1. Open Task Scheduler
2. Create a new task
3. Set trigger: "At system startup"
4. Set action: Start program `python.exe` with argument pointing to your script
5. Configure to run whether user is logged in or not

### Using nohup (Simple background process)

```bash
nohup python bot_exception_automator_v2.py > output.log 2>&1 &
```

## Troubleshooting

### Connection Issues

**Error**: `Database connection failed`

**Solutions**:
- Verify database credentials in `config.py`
- Check database server is running
- Verify network connectivity
- Check firewall settings
- Ensure PostgreSQL is accepting connections

### No Updates Happening

**Possible causes**:
- No new exceptions in the database
- All inventory IDs already processed today
- Database permissions insufficient

**Check**:
- Review log file for details
- Manually run the SELECT queries to verify exceptions exist
- Verify UPDATE permissions on the table

### Script Stops Unexpectedly

**Solutions**:
- Check log file for error messages
- Ensure database connection remains stable
- Use systemd or other service manager for auto-restart

## Customization

### Change Check Interval

Edit `config.py`:
```python
CHECK_INTERVAL = 15  # Check every 15 minutes instead of 10
```

### Add More Process Types

Modify the `process_exceptions()` method in the script to add more process types:

```python
# Add process type 3
type3_ids = self.fetch_exception_inventory_ids('3')
all_inventory_ids.extend(type3_ids)
```

### Modify Query Criteria

Edit the `fetch_exception_inventory_ids()` method to change query filters.

## Security Notes

⚠️ **Important Security Practices**:
- Never commit `config.py` to version control (add to `.gitignore`)
- Use environment variables for sensitive credentials in production
- Ensure database user has minimal required permissions
- Regularly rotate database passwords
- Monitor log files for unauthorized access attempts

## Support

For issues or questions:
1. Check the log file for error details
2. Verify database connectivity manually
3. Ensure all prerequisites are installed
4. Review this README for common solutions

## Files Included

- `bot_exception_automator.py` - Standalone version with hardcoded config
- `bot_exception_automator_v2.py` - Enhanced version with config file support (Recommended)
- `config_template.py` - Template for database configuration
- `README.md` - This documentation file

## Version History

- **v2.0** - Added config file support, date change detection, enhanced logging
- **v1.0** - Initial release with basic functionality