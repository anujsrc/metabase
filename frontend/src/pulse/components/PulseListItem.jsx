import React, { Component, PropTypes } from "react";

import PulseModal from "./PulseModal.jsx";

import ModalWithTrigger from "metabase/components/ModalWithTrigger.jsx";

import _ from "underscore";

function formatSchedule(channels) {
    let types = {};
    for (let c of channels) {
        types[c.type] = types[c.type] || [];
        types[c.type].push(c);
    }
    return Object.keys(types).map(type => formatChannel(type, types[type])).join(" and ");
}

function formatChannel(type, channels) {
    switch (type) {
        case "email":
            return "Emailed " + formatList(_.uniq(channels.map(c => c.schedule)) );
        case "slack":
            return "Slack'd to " + formatList(channels.map(c => c.channel + " " + c.schedule))
        default:
            return "Sent to " + type + " " + formatList(_.uniq(channels.map(c => c.schedule)));
    }
}

function formatList(list) {
    return list.slice(0, -1).join(", ") + (list.length < 2 ? "" : ", and ") + list[list.length - 1];
}

export default class PulseListItem extends Component {
    constructor(props) {
        super(props);
        this.state = {};
    }

    static propTypes = {};
    static defaultProps = {};

    render() {
        let { pulse } = this.props;

        return (
            <div className="bordered rounded mb3 px4 py3">
                <div className="flex mb2">
                    <div>
                        <h2 className="mb1">{pulse.name}</h2>
                        <span>Pulse by <span className="text-bold">{pulse.creator}</span></span>
                    </div>
                    <div className="flex-align-right">
                        { pulse.subscribed ?
                            <button className="Button Button--primary">You recieve this pulse</button>
                        :
                            <button className="Button">Get this pulse</button>
                        }
                        <ModalWithTrigger
                            ref="editPulseModal"
                            triggerClasses="Button ml1"
                            triggerElement="Edit"
                        >
                            <PulseModal
                                pulse={pulse}
                                onClose={() => this.refs.editPulseModal.close()}
                                onSave={this.props.onSave}
                            />
                        </ModalWithTrigger>
                    </div>
                </div>
                <ol className="mb2">
                    { pulse.cards.map(card =>
                        <li className="Button mr1">
                            {card}
                        </li>
                    )}
                </ol>
                <div>
                    {formatSchedule(pulse.channels)}
                </div>
            </div>
        );
    }
}