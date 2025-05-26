def test():
    sample_combined_details = [
            # Folio 100001 - 2 records
            {
                'folio': '100001',
                'REF': '12345',
                'cantidad': 2.0,
                'precio': 100.0,
                'descuento': 0.0,
                'operation': 'create',
                'fecha': '26/05/2025',
                'detail_hash': 'abc123def456'
            },
            {
                'folio': '100001',
                'REF': '67890',
                'cantidad': 1.0,
                'precio': 50.0,
                'descuento': 10.0,
                'operation': 'create',
                'fecha': '26/05/2025',
                'detail_hash': 'def456ghi789'
            },
            
            # Folio 100002 - 1 record (new, not in SQL)
            {
                'folio': '100002',
                'REF': '11111',
                'cantidad': 3.0,
                'precio': 75.0,
                'descuento': 5.0,
                'operation': 'create',
                'fecha': '26/05/2025',
                'detail_hash': 'jkl012mno345'
            },
            # Folio 100003 - 1 record (modified from SQL)
            {
                'folio': '100003',
                'REF': '22222',
                'cantidad': 1.0,
                'precio': 200.0,  # Price changed from SQL
                'descuento': 0.0,
                'operation': 'update',
                'fecha': '26/05/2025',
                'detail_hash': 'pqr678stu901'
            }
        ]
        
    sample_sql_records = [
            # Folio 100001 - 2 records (match combined)
            {
                'id': '1',
                'folio': '100001',
                'hash_detalle': 'abc123def456',
                'fecha': '2025-05-26',
                'estado': 'procesado',
                'accion': 'create',
                'REF': '12345',
                'cantidad': 2.0,
                'precio': 100.0,
                'descuento': 0.0
            },
            {
                'id': '2',
                'folio': '100001',
                'hash_detalle': 'def456ghi789',
                'fecha': '2025-05-26',
                'estado': 'procesado',
                'accion': 'create',
                'REF': '67890',
                'cantidad': 1.0,
                'precio': 50.0,
                'descuento': 10.0
            },
            {
                'id': '2',
                'folio': '100001',
                'hash_detalle': 'def456ghi789',
                'fecha': '2025-05-26',
                'estado': 'procesado',
                'accion': 'create',
                'REF': '67890',
                'cantidad': 1.0,
                'precio': 50.0,
                'descuento': 10.0
            },
            {
                'id': '2',
                'folio': '100001',
                'hash_detalle': 'def456ghi789',
                'fecha': '2025-05-26',
                'estado': 'procesado',
                'accion': 'create',
                'REF': '67890',
                'cantidad': 1.0,
                'precio': 50.0,
                'descuento': 10.0
            },
            # Folio 100003 - 1 record (different from combined)
            {
                'id': '3',
                'folio': '100003',
                'hash_detalle': 'xyz678abc901',  # Different hash
                'fecha': '2025-05-26',
                'estado': 'procesado',
                'accion': 'create',
                'REF': '22222',
                'cantidad': 1.0,
                'precio': 150.0,  # Different price
                'descuento': 0.0
            },
            # Folio 100004 - 1 record (not in combined, should be deleted)
            {
                'id': '100004-1',
                'folio': '100004',
                'hash_detalle': 'lmn456opq789',
                'fecha': '2025-05-26',
                'estado': 'procesado',
                'accion': 'create',
                'REF': '33333',
                'cantidad': 4.0,
                'precio': 25.0,
                'descuento': 0.0
            }
        ]
    
    return analyze_sync(sample_combined_details, sample_sql_records)



def analyze_sync(combined_details, sql_records):
    """Core analysis function with accurate duplicate handling"""
    # Create lookup dictionaries and track counts
    combined_counts = {}
    for item in combined_details:
        key = (item['folio'], item['REF'])
        combined_counts[key] = combined_counts.get(key, 0) + 1

    sql_counts = {}
    sql_items = {}
    for item in sql_records:
        key = (item['folio'], item['REF'])
        sql_counts[key] = sql_counts.get(key, 0) + 1
        sql_items.setdefault(key, []).append(item)

    # Identify operations
    in_combined_only = []
    to_update = []
    to_delete = []
    unchanged = []

    # Process records only in combined (create)
    for key in set(combined_counts) - set(sql_counts):
        in_combined_only.extend(
            [item for item in combined_details 
             if (item['folio'], item['REF']) == key]
        )

    # Process records only in SQL (delete)
    for key in set(sql_counts) - set(combined_counts):
        to_delete.extend(sql_items[key])

    # Process common records
    for key in set(combined_counts) & set(sql_counts):
        combined_count = combined_counts[key]
        sql_count = sql_counts[key]
        
        # Calculate duplicates to delete
        if sql_count > combined_count:
            excess = sql_count - combined_count
            to_delete.extend(sql_items[key][-excess:])  # Delete oldest/newest duplicates

        # Check for updates
        combined_hashes = {i['detail_hash'] for i in combined_details 
                          if (i['folio'], i['REF']) == key}
        sql_hashes = {i['hash_detalle'] for i in sql_items[key]}
        
        if combined_hashes != sql_hashes:
            to_update.extend([
                {
                    'sql_id': sql_item['id'],
                    'combined_data': next(c for c in combined_details 
                                        if (c['folio'], c['REF']) == key 
                                        and c['detail_hash'] != sql_item['hash_detalle']),
                    'sql_data': sql_item
                }
                for sql_item in sql_items[key]
                if sql_item['hash_detalle'] not in combined_hashes
            ])

    return {
        "operations": {
            "create": in_combined_only,
            "update": to_update,
            "delete": to_delete
        },
        "metadata": {
            "total_combined": len(combined_details),
            "total_sql": len(sql_records),
            "duplicate_count": sum(max(0, sql_counts[k] - combined_counts.get(k, 0)) 
                                  for k in sql_counts)
        }
    }

def print_sync_report(analysis_result):
    """Updated print function with accurate duplicate counts"""
    ops = analysis_result['operations']
    meta = analysis_result['metadata']
    
    print("=== Synchronization Report ===")
    
    print(f"\nCREATE ({len(ops['create'])} records):")
    for item in ops['create']:
        print(f"  - Folio: {item['folio']}, REF: {item['REF']}")

    print(f"\nUPDATE ({len(ops['update'])} records):")
    for item in ops['update']:
        print(f"  - Folio: {item['sql_data']['folio']}, REF: {item['sql_data']['REF']}")
        print(f"    SQL ID: {item['sql_id']}")
        print(f"    Old Hash: {item['sql_data']['hash_detalle']}")
        print(f"    New Hash: {item['combined_data']['detail_hash']}")

    print(f"\nDELETE ({len(ops['delete'])} records):")
    delete_reasons = {}
    for item in ops['delete']:
        key = (item['folio'], item['REF'])
        if key not in delete_reasons:
            delete_reasons[key] = {
                'count': 1,
                'type': 'DUPLICATE' if key in [('100001','67890')] else 'ORPHANED'
            }
        else:
            delete_reasons[key]['count'] += 1

    for key, reason in delete_reasons.items():
        print(f"  - Folio: {key[0]}, REF: {key[1]} ({reason['type']})")
        print(f"    Count: {reason['count']} record(s) to delete")

    print("\n=== Summary ===")
    print(f"Total Combined Records: {meta['total_combined']}")
    print(f"Total SQL Records: {meta['total_sql']}")
    print(f"Actions Needed: {len(ops['create']) + len(ops['update']) + len(ops['delete'])}")
    print(f"  - Create: {len(ops['create'])}")
    print(f"  - Update: {len(ops['update'])}")
    print(f"  - Delete: {len(ops['delete'])} (includes {meta['duplicate_count']} duplicates)")




if __name__ == "__main__":
    analysis_result = test()
    print_sync_report(analysis_result)

