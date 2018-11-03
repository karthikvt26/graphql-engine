import React from 'react';

import { generateHeaderSyms } from './HeaderReducer';

class Header extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      ...generateHeaderSyms(props.eventPrefix),
    };
  }
  componentWillUnmount() {
    // Reset the header whenever it is unmounted
    this.props.dispatch({
      type: this.state.RESET_HEADER,
    });
  }
  getIndex(e) {
    const indexId = e.target.getAttribute('data-index-id');
    return parseInt(indexId, 10);
  }
  headerKeyChange(e) {
    const indexId = this.getIndex(e);
    if (indexId < 0) {
      console.error('Unable to handle event');
      return;
    }
    Promise.all([
      this.props.dispatch({
        type: this.state.HEADER_KEY_CHANGE,
        data: {
          name: e.target.value,
          index: indexId,
        },
      }),
    ]);
  }
  checkAndAddNew(e) {
    const indexId = this.getIndex(e);
    if (indexId < 0) {
      console.error('Unable to handle event');
      return;
    }
    if (
      this.props.headers[indexId].name &&
      this.props.headers[indexId].name.length > 0 &&
      indexId === this.props.headers.length - 1
    ) {
      Promise.all([this.props.dispatch({ type: this.state.ADD_NEW_HEADER })]);
    }
  }
  headerValueChange(e) {
    const indexId = this.getIndex(e);
    if (indexId < 0) {
      console.error('Unable to handle event');
      return;
    }
    this.props.dispatch({
      type: this.state.HEADER_VALUE_CHANGE,
      data: {
        value: e.target.value,
        index: indexId,
      },
    });
  }
  headerTypeChange(e) {
    const indexId = this.getIndex(e);
    if (indexId < 0) {
      console.error('Unable to handle event');
      return;
    }
    this.props.dispatch({
      type: this.state.HEADER_VALUE_TYPE_CHANGE,
      data: {
        type: e.target.value,
        index: indexId,
      },
    });
  }
  deleteHeader(e) {
    const indexId = this.getIndex(e);
    if (indexId < 0) {
      console.error('Unable to handle event');
      return;
    }
    this.props.dispatch({
      type: this.state.DELETE_HEADER,
      data: {
        type: e.target.value,
        index: indexId,
      },
    });
  }
  render() {
    const styles = require('./Header.scss');
    const { isDisabled } = this.props;
    const generateHeaderHtml = this.props.headers.map((h, i) => (
      <div className={styles.display_flex + ' form-group'} key={i}>
        <input
          type="text"
          className={
            styles.input +
            ' form-control ' +
            styles.add_mar_right +
            ' ' +
            styles.defaultWidth
          }
          data-index-id={i}
          value={h.name}
          onChange={this.headerKeyChange.bind(this)}
          onBlur={this.checkAndAddNew.bind(this)}
          disabled={isDisabled}
        />
        <select
          className={
            'form-control ' +
            styles.add_pad_left +
            ' ' +
            styles.add_mar_right +
            ' ' +
            styles.defaultWidth
          }
          value={h.type}
          onChange={this.headerTypeChange.bind(this)}
          data-index-id={i}
          disabled={isDisabled}
        >
          <option disabled value="">
            -- value type --
          </option>
          {this.props.typeOptions.map((o, k) => (
            <option key={k} value={o.value} data-index-id={i}>
              {o.display}
            </option>
          ))}
        </select>
        <input
          type="text"
          className={
            styles.inputDefault +
            ' form-control ' +
            styles.defaultWidth +
            ' ' +
            styles.add_pad_left
          }
          placeholder="value"
          value={h.value}
          onChange={this.headerValueChange.bind(this)}
          data-index-id={i}
          disabled={isDisabled}
        />
        {i !== this.props.headers.length - 1 && !isDisabled ? (
          <i
            className={styles.fontAwosomeClose + ' fa-lg fa fa-times'}
            onClick={this.deleteHeader.bind(this)}
            data-index-id={i}
          />
        ) : null}
      </div>
    ));
    return <div className={this.props.wrapper_class}>{generateHeaderHtml}</div>;
  }
}

// Add proptypes

export default Header;