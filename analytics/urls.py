from django.urls import path
from .views import (
    SalesOverTimeView,
    SalesGrowthRateView,
    NewCustomersOverTimeView,
    RepeatCustomersView,
    GeographicalDistributionView,
    CustomerLifetimeValueView
)

urlpatterns = [
    path('sales-over-time/', SalesOverTimeView.as_view(), name='sales-over-time'),
    path('sales-growth-rate/', SalesGrowthRateView.as_view(), name='sales-growth-rate'),
    path('new-customers-over-time/', NewCustomersOverTimeView.as_view(), name='new-customers-over-time'),
    path('repeat-customers/', RepeatCustomersView.as_view(), name='repeat-customers'),
    path('geographical-distribution/', GeographicalDistributionView.as_view(), name='geographical-distribution'),
    path('customer-lifetime-value/', CustomerLifetimeValueView.as_view(), name='customer-lifetime-value'),
]
