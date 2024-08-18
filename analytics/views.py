from django.conf import settings
from django.http import JsonResponse
from rest_framework.views import APIView

import logging
logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

import itertools
# Sales Over Time
class SalesOverTimeView(APIView):
    def get(self, request, format=None):
        orders = settings.MONGO_DB['shopifyOrders']
        pipeline = [
            {
                '$addFields': {
                    'created_at': {
                        '$toDate': '$created_at'
                    },
                    'total_price': {
                        '$toDouble': '$total_price_set.shop_money.amount'
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$created_at'},
                        'month': {'$month': '$created_at'},
                    },
                    'total_sales': {'$sum': '$total_price'}
                }
            },
            {'$sort': {'_id': 1}}
        ]
        result = list(orders.aggregate(pipeline))
        return JsonResponse(result, safe=False, json_dumps_params={'indent': 2})
# Sales Growth Rate Over Time
class SalesGrowthRateView(APIView):
    def get(self, request, format=None):
        orders = settings.MONGO_DB['shopifyOrders']
        pipeline = [
            {
                '$addFields': {
                    'created_at': {
                        '$toDate': '$created_at'
                    },
                    'total_sales': {
                        '$toDouble': '$total_price_set.shop_money.amount'
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$created_at'},
                        'month': {'$month': '$created_at'},
                    },
                    'total_sales': {'$sum': '$total_sales'}
                }
            },
            {'$sort': {'_id': 1}},
            {
                '$setWindowFields': {
                    'sortBy': {'_id.year': 1, '_id.month': 1},
                    'output': {
                        'prev_sales': {
                            '$shift': {
                                'output': '$total_sales',
                                'by': -1
                            }
                        }
                    }
                }
            },
            {
                '$addFields': {
                    'growth_rate': {
                        '$cond': {
                            'if': {'$eq': ['$prev_sales', 0]},
                            'then': None,  # or some other value indicating no previous sales
                            'else': {
                                '$multiply': [
                                    {
                                        '$divide': [
                                            {'$subtract': ['$total_sales', '$prev_sales']},
                                            '$prev_sales'
                                        ]
                                    },
                                    100
                                ]
                            }
                        }
                    }
                }
            },
            {'$project': {'prev_sales': 0}}
        ]
        result = list(orders.aggregate(pipeline))
        return JsonResponse(result, safe=False, json_dumps_params={'indent': 2})

# New Customers Added Over Time
class NewCustomersOverTimeView(APIView):
    def get(self, request, format=None):
        customers = settings.MONGO_DB['shopifyCustomers']
        pipeline = [
            {
                '$addFields': {
                    'created_at': {
                        '$toDate': '$created_at'
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$created_at'},
                        'month': {'$month': '$created_at'},
                    },
                    'new_customers': {'$sum': 1}
                }
            },
            {'$sort': {'_id': 1}}
        ]
        result = list(customers.aggregate(pipeline))
        return JsonResponse(result, safe=False, json_dumps_params={'indent': 2})

# Geographical Distribution of Customers
class GeographicalDistributionView(APIView):
    def get(self, request, format=None):
        customers = settings.MONGO_DB['shopifyCustomers']
        pipeline = [
            {
                '$group': {
                    '_id': '$default_address.city',
                    'customer_count': {'$sum': 1}
                }
            },
            {'$sort': {'customer_count': -1}}
        ]
        result = list(customers.aggregate(pipeline))
        return JsonResponse(result, safe=False, json_dumps_params={'indent': 2})

class CustomerLifetimeValueView(APIView):
    def get(self, request, format=None):
        orders = settings.MONGO_DB['shopifyOrders']

        pipeline = [
            {
                '$addFields': {
                    'created_at': {
                        '$toDate': '$created_at'
                    },
                    # Convert total_price_set.shop_money.amount to a number
                    'total_price_amount': {
                        '$toDouble': '$total_price_set.shop_money.amount'
                    }
                }
            },
            # Group by customer_id and find the first order date
            {
                '$group': {
                    '_id': '$customer.id',
                    'first_order_date': { '$min': '$created_at' },
                    'lifetime_value': { '$sum': '$total_price_amount' }
                }
            },
            # Group by the month and year of the first order date
            {
                '$group': {
                    '_id': {
                        'year': { '$year': '$first_order_date' },
                        'month': { '$month': '$first_order_date' }
                    },
                    'avg_lifetime_value': { '$avg': '$lifetime_value' }
                }
            },
            # Sort by year and month
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.month': 1
                }
            }
        ]

        result = list(orders.aggregate(pipeline))
        return JsonResponse(result, safe=False, json_dumps_params={'indent': 2})


class RepeatCustomersView(APIView):
    def get(self, request, format=None):
        orders = settings.MONGO_DB['shopifyOrders']
        
        # Get the range of years in the dataset
        date_range = orders.aggregate([
            {
                '$group': {
                    '_id': None,
                    'minDate': {'$min': {'$dateFromString': {'dateString': '$created_at'}}},
                    'maxDate': {'$max': {'$dateFromString': {'dateString': '$created_at'}}}
                }
            }
        ])
        
        date_range = list(date_range)[0]
        min_date = date_range['minDate']
        max_date = date_range['maxDate']
        
        def generate_time_series(start_date, end_date, period):
            current_date = start_date
            while current_date <= end_date:
                if period == 'daily':
                    yield {'year': current_date.year, 'month': current_date.month, 'day': current_date.day}
                    current_date += timedelta(days=1)
                elif period == 'monthly':
                    yield {'year': current_date.year, 'month': current_date.month}
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1)
                elif period == 'quarterly':
                    yield {'year': current_date.year, 'quarter': (current_date.month - 1) // 3 + 1}
                    if current_date.month + 3 > 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 3)
                elif period == 'yearly':
                    yield {'year': current_date.year}
                    current_date = current_date.replace(year=current_date.year + 1)

        def complete_time_series(results, time_series, period):
            results_dict = {(r['_id']['year'], r['_id'].get('month', None), r['_id'].get('day', None), r['_id'].get('quarter', None)): r['repeat_customers'] for r in results}
            completed_results = []
            for period_key in time_series:
                result_key = (period_key['year'], period_key.get('month', None), period_key.get('day', None), period_key.get('quarter', None))
                completed_results.append({
                    '_id': period_key,
                    'repeat_customers': results_dict.get(result_key, 0)
                })
            return completed_results

        # Common part of the pipeline to add date information
        common_pipeline = [
            {
                '$addFields': {
                    'created_at': {
                        '$dateFromString': {'dateString': '$created_at'}
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'customer_id': '$customer.id',
                        'year': {'$year': '$created_at'},
                        'month': {'$month': '$created_at'},
                        'day': {'$dayOfMonth': '$created_at'},
                        'quarter': {'$ceil': {'$divide': [{'$month': '$created_at'}, 3]}}
                    },
                    'order_count': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'order_count': {'$gt': 1}
                }
            }
        ]

        # Generate time series data
        daily_time_series = list(generate_time_series(min_date, max_date, 'daily'))
        monthly_time_series = list(generate_time_series(min_date, max_date, 'monthly'))
        quarterly_time_series = list(generate_time_series(min_date, max_date, 'quarterly'))
        yearly_time_series = list(generate_time_series(min_date, max_date, 'yearly'))

        # Pipeline for daily repeat customers
        daily_pipeline = common_pipeline + [
            {
                '$group': {
                    '_id': {
                        'year': '$_id.year',
                        'month': '$_id.month',
                        'day': '$_id.day'
                    },
                    'repeat_customers': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.month': 1,
                    '_id.day': 1
                }
            }
        ]

        # Pipeline for monthly repeat customers
        monthly_pipeline = common_pipeline + [
            {
                '$group': {
                    '_id': {
                        'year': '$_id.year',
                        'month': '$_id.month'
                    },
                    'repeat_customers': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.month': 1
                }
            }
        ]

        # Pipeline for quarterly repeat customers
        quarterly_pipeline = common_pipeline + [
            {
                '$group': {
                    '_id': {
                        'year': '$_id.year',
                        'quarter': '$_id.quarter'
                    },
                    'repeat_customers': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.quarter': 1
                }
            }
        ]

        # Pipeline for yearly repeat customers
        yearly_pipeline = common_pipeline + [
            {
                '$group': {
                    '_id': {
                        'year': '$_id.year'
                    },
                    'repeat_customers': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1
                }
            }
        ]

        # Executing the pipelines
        daily_result = complete_time_series(list(orders.aggregate(daily_pipeline)), daily_time_series, 'daily')
        monthly_result = complete_time_series(list(orders.aggregate(monthly_pipeline)), monthly_time_series, 'monthly')
        quarterly_result = complete_time_series(list(orders.aggregate(quarterly_pipeline)), quarterly_time_series, 'quarterly')
        yearly_result = complete_time_series(list(orders.aggregate(yearly_pipeline)), yearly_time_series, 'yearly')

        result = {
            'daily': daily_result,
            'monthly': monthly_result,
            'quarterly': quarterly_result,
            'yearly': yearly_result
        }

        return JsonResponse(result, safe=False, json_dumps_params={'indent': 2})