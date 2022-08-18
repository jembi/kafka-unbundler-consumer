import { expect } from 'chai';
import { splitResources } from '../utils';
import { Bundle } from '..';

const emptyMockMessages: Bundle = {
  entry: [],
};

const emptyResults = {};

const mockMessages: Bundle = {
  entry: [
    {
      resource: {
        resourceType: 'Patient',
        id: 'test-1-1',
      },
    },
    {
      resource: {
        resourceType: 'Patient',
        id: 'test-1-2',
      },
    },
    {
      resource: {
        resourceType: 'Observation',
        id: 'test-2-1',
      },
    },
  ],
};

const splitResults = {
  Patient: [
    {
      key: 'test-1-1',
      value: '{"resource":{"resourceType":"Patient","id":"test-1-1"}}',
    },
    {
      key: 'test-1-2',
      value: '{"resource":{"resourceType":"Patient","id":"test-1-2"}}',
    },
  ],
  Observation: [
    {
      key: 'test-2-1',
      value: '{"resource":{"resourceType":"Observation","id":"test-2-1"}}',
    },
  ],
};

const mockMessagesOneType: Bundle = {
  entry: [
    {
      resource: {
        resourceType: 'Patient',
        id: 'test-1-1',
      },
    },
    {
      resource: {
        resourceType: 'Patient',
        id: 'test-1-2',
      },
    },
  ],
};

const oneResourceTypeResults = {
  Patient: [
    {
      key: 'test-1-1',
      value: '{"resource":{"resourceType":"Patient","id":"test-1-1"}}',
    },
    {
      key: 'test-1-2',
      value: '{"resource":{"resourceType":"Patient","id":"test-1-2"}}',
    },
  ],
};

describe('Kafka Unbundler Consumer Unit Tests', function () {
  describe('Test splitResources function', function () {
    it('Should return empty objects for empty entries', function () {
      const results = splitResources(emptyMockMessages);
      expect(results).to.be.empty;
      expect(results).to.eql(emptyResults);
    });

    it('Should group one type of resource', function () {
      const results = splitResources(mockMessagesOneType);
      expect(Object.keys(results).length).equal(1);
      expect(results).to.eql(oneResourceTypeResults);
    });

    it('Should split different resources', function () {
      const results = splitResources(mockMessages);
      expect(Object.keys(results).length).equal(2);
      expect(results).to.eql(splitResults);
    });
  });
});
