package org.araqne.logdb;

public enum QueryStopReason {
	End, UserRequest, PartialFetch, CommandFailure, LowDisk;
}