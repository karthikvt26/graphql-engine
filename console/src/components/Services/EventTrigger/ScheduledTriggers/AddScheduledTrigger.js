import React from 'react';
import Helmet from 'react-helmet';
import OverlayTrigger from 'react-bootstrap/lib/OverlayTrigger';
import * as tooltip from './Tooltips';
import Button from '../../../Common/Button/Button';
import { push } from 'react-router-redux';

import globals from '../../../../Globals';

import {
  showSuccessNotification,
  showErrorNotification,
} from '../../Common/Notification';

const appPrefix = globals.urlPrefix + '/events';

import {
  updateInput,
  createScheduledTrigger,
  resetAdd,
} from './AddScheduledTriggerActions';

import DropdownButton from '../../../Common/DropdownButton/DropdownButton';

import {
  options,
  CRON_TYPE,
  CRON_TYPE_VALUE,
  ONE_OFF_TYPE,
  ONE_OFF_TYPE_VALUE,
} from './constants';

const AddScheduledTrigger = props => {
  const { dispatch, addScheduledTrigger } = props;
  const {
    triggerName,
    webhookUrl,
    scheduleType,
    scheduleValue,
  } = addScheduledTrigger;
  const styles = require('../TableCommon/EventTable.scss');
  const createBtnText = 'Create Scheduled Trigger';
  /*
  if (ongoingRequest) {
    createBtnText = 'Creating...';
  } else if (lastError) {
    createBtnText = 'Creating Failed. Try again';
  } else if (internalError) {
    createBtnText = 'Creating Failed. Try again';
  } else if (lastSuccess) {
    createBtnText = 'Created! Redirecting...';
  }
  */
  const onInputChange = e => {
    const fieldName = e.target.getAttribute('data-field-name');
    if (fieldName && fieldName.length !== 0) {
      return dispatch(updateInput(fieldName, e.target.value));
    }
  };
  const invokeCreateScheduledTrigger = e => {
    e.preventDefault();
    dispatch(showSuccessNotification('Creating trigger!'));
    dispatch(createScheduledTrigger())
      .then(() => {
        dispatch(showSuccessNotification('Trigger created successfully!'));
        dispatch(resetAdd());
        dispatch(push(`${appPrefix}/scheduled-triggers`));
      })
      .catch(err => {
        dispatch(
          showErrorNotification('Error creating trigger', JSON.stringify(err))
        );
      });
  };
  return (
    <div className={`${styles.clear_fix}`}>
      <Helmet title="Create Trigger - Events | Hasura" />
      <div className={styles.subHeader}>
        <h2 className={styles.heading_text}>Create a new trigger</h2>
        <div className="clearfix" />
      </div>
      <br />
      <div className={`container-fluid ${styles.padd_left_remove}`}>
        <form onSubmit={invokeCreateScheduledTrigger}>
          <div
            className={`${styles.addCol} col-xs-12 ${styles.padd_left_remove}`}
          >
            <h4 className={styles.subheading_text}>
              Trigger Name &nbsp; &nbsp;
              <OverlayTrigger
                placement="right"
                overlay={tooltip.scheduledTriggerName}
              >
                <i className="fa fa-question-circle" aria-hidden="true" />
              </OverlayTrigger>{' '}
            </h4>
            <input
              type="text"
              data-test="scheduled-trigger-name"
              placeholder="trigger_name"
              required
              pattern="^[A-Za-z]+[A-Za-z0-9_\\-]*$"
              className={`${styles.tableNameInput} form-control`}
              data-field-name="triggerName"
              onChange={onInputChange}
              value={triggerName}
            />
            <hr />
            <h4 className={styles.subheading_text}>
              Webhook URL &nbsp; &nbsp;
              <OverlayTrigger
                placement="right"
                overlay={tooltip.scheduledTriggerWebhook}
              >
                <i className="fa fa-question-circle" aria-hidden="true" />
              </OverlayTrigger>{' '}
            </h4>
            <input
              type="text"
              data-test="scheduled-trigger-webhook-url"
              placeholder="webhook url"
              required
              className={`${styles.tableNameInput} form-control`}
              data-field-name="webhookUrl"
              onChange={onInputChange}
              value={webhookUrl}
            />
            <hr />
            <h4 className={styles.subheading_text}>
              Schedule &nbsp; &nbsp;
              <OverlayTrigger
                placement="right"
                overlay={tooltip.scheduledTriggerWebhook}
              >
                <i className="fa fa-question-circle" aria-hidden="true" />
              </OverlayTrigger>{' '}
            </h4>
            <div className={`${styles.display_flex} form-group`}>
              <div className={styles.dropDownGroup}>
                <DropdownButton
                  dropdownOptions={options}
                  title={
                    scheduleType === CRON_TYPE
                      ? CRON_TYPE_VALUE
                      : ONE_OFF_TYPE_VALUE
                  }
                  dataKey={
                    scheduleType === CRON_TYPE ? CRON_TYPE : ONE_OFF_TYPE
                  }
                  onButtonChange={e => {
                    dispatch(
                      updateInput(
                        'scheduleType',
                        e.target.getAttribute('value')
                      )
                    );
                  }}
                  onInputChange={e => {
                    dispatch(updateInput('scheduleValue', e.target.value));
                  }}
                  bsClass={styles.dropdown_button}
                  id="schedule-type"
                  inputVal={scheduleValue}
                  testId="schedule-type"
                  inputPlaceHolder={
                    scheduleType === CRON_TYPE ? '* * * * 5' : 'date'
                  }
                />
              </div>
            </div>
            <hr />
            <Button
              type="submit"
              color="yellow"
              size="sm"
              data-test="trigger-create"
              disabled={
                !triggerName || !webhookUrl || !scheduleType || !scheduleValue
              }
            >
              {createBtnText}
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
};

const mapStateToProps = state => {
  return {
    addScheduledTrigger: { ...state.scheduledTrigger.addScheduledTrigger },
  };
};

const addScheduledTriggerConnector = connect =>
  connect(mapStateToProps)(AddScheduledTrigger);

export default addScheduledTriggerConnector;