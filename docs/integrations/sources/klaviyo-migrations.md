# Klaviyo Migration Guide

## Upgrading to 3.0.0

We're continuously striving to enhance the quality and reliability of our connectors at Airbyte.
As part of our commitment to delivering exceptional service, we are transitioning Source Sendgrid from the Python Connector Development Kit (CDK)
to our new low-code framework improving maintainability and reliability of the connector.
However, due to differences between the Python and low-code CDKs, this migration constitutes a breaking change.

This release contains schema changes for streams `campaigns`, `global_exclusions`, and `profiles`.
Users will need to refresh the source schemas and reset these streams after upgrading.

## Connector Upgrade Guide

### For Airbyte Open Source: Update the local connector image

Airbyte Open Source users must manually update the connector image in their local registry before proceeding with the migration. To do so:

1. Select **Settings** in the main navbar.
    1. Select **Sources**.
2. Find Klaviyo in the list of connectors. 

:::note
You will see two versions listed, the current in-use version and the latest version available.
::: 

3. Select **Change** to update your OSS version to the latest available version.


### Update the connector version

1. Select **Sources** in the main navbar. 
2. Select the instance of the connector you wish to upgrade.

:::note
Each instance of the connector must be updated separately. If you have created multiple instances of a connector, updating one will not affect the others.
:::

3. Select **Upgrade**
    1. Follow the prompt to confirm you are ready to upgrade to the new version.

### Refresh affected schemas and reset data

1. Select **Connections** in the main nav bar.
    1. Select the connection(s) affected by the update.
2. Select the **Replication** tab.
    1. Select **Refresh source schema**.
    2. Select **OK**.
:::note
Any detected schema changes will be listed for your review.
:::
3. Select **Save changes** at the bottom of the page.
    1. Ensure the **Reset affected streams** option is checked.
:::note
Depending on destination type you may not be prompted to reset your data.
:::
4. Select **Save connection**. 
:::note
This will reset the data in your destination and initiate a fresh sync.
:::

For more information on resetting your data in Airbyte, see [this page](https://docs.airbyte.com/operator-guides/reset).


## Upgrading to 2.0.0

Streams `campaigns`, `email_templates`, `events`, `flows`, `global_exclusions`, `lists`, and `metrics` are now pulling
data using latest API which has a different schema. Users will need to refresh the source schemas and reset these
streams after upgrading. See the chart below for the API version change.

| Stream            | Current API version | New API version |
|-------------------|---------------------|-----------------|
| campaigns         | v1                  | 2023-06-15      |
| email_templates   | v1                  | 2023-10-15      |
| events            | v1                  | 2023-10-15      |
| flows             | v1                  | 2023-10-15      |
| global_exclusions | v1                  | 2023-10-15      |
| lists             | v1                  | 2023-10-15      |
| metrics           | v1                  | 2023-10-15      |
| profiles          | 2023-02-22          | 2023-02-22      |


## Upgrading to 1.0.0

`event_properties/items/quantity` for `Events` stream is changed from `integer` to `number`.
For a smooth migration, data reset and schema refresh are needed.