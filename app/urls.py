from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from . import views

urlpatterns = [
    path('csv2head/', views.Csv2Head.as_view()),
    path('csvtransform/', views.CsvTransform.as_view()),
    path('database2schema/', views.Database2Schema.as_view()),
    path('databasetransform/', views.DatabaseTransform.as_view()),
]

urlpatterns = format_suffix_patterns(urlpatterns)
