{% snapshot dim_mint_metadata %}

{{
    config(
      target_schema='silver',
      strategy='check',
      unique_key='mint_address',
      check_cols=[
          'token_symbol', 
          'token_name', 
          'token_decimals', 
          'logo_uri', 
          'website_url', 
          'twitter_handle',
          'medium_url'
        ]
    )
}}

select * from {{ ref('stg_mint_metadata') }}

{% endsnapshot %}
